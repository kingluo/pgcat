package pgcat

import (
	"context"
	"database/sql/driver"
	"fmt"
	"net"
	"runtime/debug"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/jackc/pgx"
	"github.com/jackc/pgx/log/zapadapter"
	"github.com/jackc/pgx/pgtype"
	"github.com/kyleconroy/pgoutput"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"golang.org/x/net/proxy"
)

type applyInfo struct {
	localLSN, remoteLSN uint64
	prev, next          *applyInfo
}

type pgLSN uint64

func (lsn *pgLSN) Scan(src interface{}) error {
	switch val := src.(type) {
	case int64:
		*lsn = pgLSN(val)
	case string:
		v, err := pgx.ParseLSN(val)
		if err != nil {
			return err
		}
		*lsn = pgLSN(v)
	default:
		return fmt.Errorf("cannot decode as pg_lsn, src=%+v", src)
	}

	return nil
}

func (lsn *pgLSN) Value() (driver.Value, error) {
	return pgx.FormatLSN(uint64(*lsn)), nil
}

func pluginArgs(version string, publications []string) string {
	return fmt.Sprintf(`"proto_version" '%s', "publication_names" '%s'`, version, strings.Join(publications, ","))
}

func (state *applyState) sendFeedback(replyRequested bool) {
	var flushLSN pgLSN
	row := state.applyConn.QueryRow("select pg_current_wal_flush_lsn()")
	if err := row.Scan(&flushLSN); err != nil {
		state.Panic(err)
	}

	var havingPendingTxes bool
	var writePos, flushPos uint64
	for applyInfo := state.applyInfoList.next; applyInfo != state.applyInfoList; applyInfo = applyInfo.next {
		writePos = applyInfo.remoteLSN
		if applyInfo.localLSN <= uint64(flushLSN) {
			applyInfo.next.prev = applyInfo.prev
			applyInfo.prev.next = applyInfo.next
			flushPos = applyInfo.remoteLSN
		} else {
			writePos = state.applyInfoList.prev.remoteLSN
			havingPendingTxes = true
			break
		}
	}

	if !havingPendingTxes {
		flushPos = state.lastRecvPos
		writePos = state.lastRecvPos
	}

	if writePos < state.lastWritePos {
		writePos = state.lastWritePos
	}

	if flushPos < state.lastFlushPos {
		flushPos = state.lastFlushPos
	}

	state.Debugw("send standby status",
		"flushPos", flushPos,
		"writePos", writePos,
		"lastRecvPos", state.lastRecvPos)
	k, err := pgx.NewStandbyStatus(flushPos, writePos, state.lastRecvPos)
	if replyRequested {
		k.ReplyRequested = 1
	}
	if err != nil {
		state.Panicf("error creating status: %s", err)
	}

	if err := state.repConn.SendStandbyStatus(k); err != nil {
		state.Panicf("error creating status: %s", err)
	}

	if writePos > state.lastWritePos {
		state.lastWritePos = writePos
	}
	if flushPos > state.lastFlushPos {
		state.lastFlushPos = flushPos
	}
}

type applyState struct {
	*zap.SugaredLogger
	sub             *subscription
	applyConnConfig pgx.ConnConfig
	repConnConfig   pgx.ConnConfig
	isSync          bool

	relation   *relationState
	localTable *localTableState

	syncCh  chan interface{}
	applyCh chan interface{}
	closeCh chan struct{}

	stopCh   chan struct{}
	stopTime time.Time

	startPos uint64
	endPos   uint64

	lastRecvPos  uint64
	lastWritePos uint64
	lastFlushPos uint64

	// points to commit record
	// used to skip transaction when conflict
	commitLsn uint64

	applyConn  *pgx.Conn
	repConn    *pgx.ReplicationConn
	applyTx    *pgx.Tx
	slotName   string
	originName string

	relations     map[uint32]*relationState
	localTables   map[string]*localTableState
	applyInfoList *applyInfo

	insertTemplate *template.Template
	updateTemplate *template.Template
	deleteTemplate *template.Template

	maxSyncWorkers int
	syncWorkers    map[uint32]*applyState
	syncWg         sync.WaitGroup
}

func (state *applyState) setupOrigin() error {
	tx, _ := state.applyConn.Begin()
	originName := state.sub.Name

	if state.isSync {
		originName = fmt.Sprintf("%s_sync_%d", state.sub.Name, state.relation.ID)
	}

	row := tx.QueryRow("select pgcat_replication_origin_oid($1) IS NULL", originName)
	var originNotExist bool
	if err := row.Scan(&originNotExist); err != nil {
		return errors.WithMessagef(err, "check origin failed, origin=%s", originName)
	}

	if originNotExist {
		var oid pgtype.OID
		row2 := tx.QueryRow("select pgcat_replication_origin_create($1)", originName)
		if err := row2.Scan(&oid); err != nil {
			return errors.WithMessagef(err, "create origin failed, origin=%s", originName)
		}
	}

	state.originName = originName

	_, err := tx.Exec("select pgcat_replication_origin_session_setup($1)", originName)
	if err != nil {
		return errors.WithMessagef(err, "setup session origin, origin=%s", originName)
	}

	if !state.isSync {
		var startLsn pgLSN
		row := tx.QueryRow("select lsn from pgcat_subscription_progress where subscription=$1", state.sub.Name)
		if err := row.Scan(&startLsn); err != nil && err != pgx.ErrNoRows {
			return err
		}
		state.startPos = uint64(startLsn)
		state.Infof("start from lsn: %d", startLsn)
	}

	return tx.Commit()
}

const pgErrorDuplicateObject = "42710"
const pgErrorInFailedSQLTransaction = "25P02"

func (state *applyState) run() {
	state.insertTemplate = template.Must(template.New("insertSql").Parse(insertSQL))
	state.updateTemplate = template.Must(template.New("updateSql").Parse(updateSQL))
	state.deleteTemplate = template.Must(template.New("deleteSql").Parse(deleteSQL))

	state.Debugw("start subscription")

	// log notice
	state.applyConnConfig.OnNotice = func(conn *pgx.Conn, notice *pgx.Notice) {
		state.Warnf("NOTICE: %+v", notice)
	}

	// create apply connection and setup origin
	var err error
	state.applyConn, err = pgx.Connect(state.applyConnConfig)
	if err != nil {
		state.Panicw("connect failed", "err", err)
	}
	defer state.applyConn.Close()

	if _, err := state.applyConn.Exec(
		"select pgcat_set_session_replication_role()"); err != nil {
		state.Panic(err)
	}

	if err := state.setupOrigin(); err != nil {
		state.Panicw("setup apply conn failed", "err", err)
	}

	defer func() {
		if state.isSync {
			_, err := state.applyConn.Exec(
				`select pgcat_replication_origin_session_reset()`)
			if err != nil {
				pgErr := err.(pgx.PgError)
				if pgErr.Code == pgErrorInFailedSQLTransaction {
					state.Warnw("pgcat_replication_origin_session_reset failed",
						"err", err)
				} else {
					state.Panicw("pgcat_replication_origin_session_reset failed",
						"err", err)
				}
			} else {
				_, err = state.applyConn.Exec(
					`select pgcat_replication_origin_drop($1)`, state.originName)
				if err != nil {
					state.Panicw("remove temporary origin failed",
						"origin", state.originName, "err", err)
				}
			}
		}
	}()

	// start replication

	state.repConnConfig = pgx.ConnConfig{
		Host:     state.sub.Hostname,
		Port:     state.sub.Port,
		Database: state.sub.Dbname,
		User:     state.sub.Username,
		Password: state.sub.Password,
		RuntimeParams: map[string]string{
			"application_name": "pgcat",
		},
		Dial:     state.applyConnConfig.Dial,
		Logger:   zapadapter.NewLogger(zap.L()),
		LogLevel: state.applyConnConfig.LogLevel,
		OnNotice: func(conn *pgx.Conn, notice *pgx.Notice) {
			state.Warnf("replication NOTICE: %+v", notice)
		},
	}

	state.repConn, err = pgx.ReplicationConnect(state.repConnConfig)
	if err != nil {
		state.Panic(err)
	}
	defer state.repConn.Close()

	typ := pgtype.DataType{Value: &pgtype.TextArray{}, Name: "_text", OID: 1009}

	state.repConn.ConnInfo.RegisterDataType(typ)

	state.applyInfoList = new(applyInfo)
	state.applyInfoList.prev = state.applyInfoList
	state.applyInfoList.next = state.applyInfoList

	if state.isSync {
		if state.copyTable() {
			return
		}
	} else {
		state.slotName = state.sub.Name
		err = state.repConn.CreateReplicationSlot(state.slotName, "pgcat")
		if err != nil {
			pgErr := err.(pgx.PgError)
			if pgErr.Code != pgErrorDuplicateObject {
				state.Panic(pgErr)
			}
		}
	}

	// retrieve all tables in the publications
	if !state.isSync {
		state.populateRelations()
	}

	state.Debugf("start replication, slot=%s, startPos=%d", state.slotName, state.startPos)
	if err := state.repConn.StartReplication(state.slotName, state.startPos,
		-1, pluginArgs("1", state.sub.Publications)); err != nil {
		state.Panic(err)
	}

	//
	// replication message loop
	//

	state.localTables = make(map[string]*localTableState)

	state.syncWorkers = make(map[uint32]*applyState)
	defer func() {
		for _, st := range state.syncWorkers {
			close(st.closeCh)
		}
		state.syncWg.Wait()
	}()

	// start wait loop
	var repWg sync.WaitGroup
	ctx, cancelFn := context.WithCancel(context.Background())
	defer func() {
		cancelFn()
		repWg.Wait()
	}()

	repCloseCh := make(chan struct{})
	defer close(repCloseCh)
	repMsgCh := make(chan *pgx.ReplicationMessage, 100)

	repWg.Add(1)
	go func() {
		defer repWg.Done()
		defer close(repMsgCh)
		for {
			msg, err := state.repConn.WaitForReplicationMessage(ctx)
			if err == context.Canceled {
				return
			}
			if err != nil {
				state.Panicf("%+v %+v", err, state)
			}

			if msg == nil {
				state.Warnw("empty msg")
				continue
			}

			select {
			case repMsgCh <- msg:
			case <-repCloseCh:
				return
			}
		}
	}()

	// we do copy above, to get lastest lsn immediately,
	// we send empty standby status to request heartbeat.
	if state.isSync {
		state.sendFeedback(true)
	}

	refreshPubTicker := time.NewTicker(3 * time.Minute)
	defer refreshPubTicker.Stop()

	for {
		var repMsg *pgx.ReplicationMessage

		if !state.isSync && state.sub.CopyData && state.applyTx == nil {
			select {
			case <-refreshPubTicker.C:
				state.populateRelations()
			default:
			}

			// start sync goroutines
			state.startTableSync()

			// wait for catchup or message
			var ok bool
			select {
			case st := <-state.applyCh:
				var catchup int
				for {
					st2 := st.(*applyState)
					relSt := st2.relation.state
					// save the reported state
					switch relSt {
					case relStateSyncCatchUp:
						catchup++
						st2.syncCh <- state.lastRecvPos
					case relStateSyncDone, relStateSyncFailed:
						delete(state.syncWorkers, st2.relation.ID)
						if state.relations[st2.relation.ID].state == relStateSyncCatchUp {
							catchup--
						}
					}
					state.relations[st2.relation.ID].state = relSt
					if relSt == relStateSyncFailed {
						state.relations[st2.relation.ID].failedTime = st2.relation.failedTime
					}
					if catchup == 0 {
						break
					}
					st = <-state.applyCh
				}
				continue
			case repMsg, ok = <-repMsgCh:
				if !ok {
					state.Panic("wait replication msg failed")
				}
			case <-state.closeCh:
				return
			}
		} else {
			var ok bool
			select {
			case repMsg, ok = <-repMsgCh:
				if !ok {
					state.Panic("wait replication msg failed")
				}
			case <-state.closeCh:
				return
			}
		}

		if walMsg := repMsg.WalMessage; walMsg != nil {
			startLSN := walMsg.WalStart
			endLSN := walMsg.ServerWalEnd
			if state.lastRecvPos < startLSN {
				state.lastRecvPos = startLSN
			}
			if state.lastRecvPos < endLSN {
				state.lastRecvPos = endLSN
			}

			msg, err := pgoutput.Parse(walMsg.WalData)
			if err != nil {
				state.Panic(err)
			}

			switch v := msg.(type) {
			case pgoutput.Relation:
				state.Debugf("msg type=Relation %+v", v)
				state.handleRelation(v)
			case pgoutput.Begin:
				state.commitLsn = v.LSN
				state.Debugf("msg type=Begin, lsn=%d %+v", walMsg.WalStart, v)
				if state.isSync {
					continue
				}

				state.applyTx, err = state.applyConn.Begin()
				if err != nil {
					state.Panic(err)
				}

				// invalidate all local tables
				for _, t := range state.localTables {
					t.localInSync = false
				}
			case pgoutput.Commit:
				state.commitLsn = 0
				state.Debugf("msg type=Commit %+v", v)
				if state.isSync {
					if state.lastRecvPos < state.endPos {
						continue
					} else {
						state.syncDone()
						return
					}
				}

				// save progress
				lsn := pgLSN(v.TransactionLSN)
				if _, err := state.applyTx.Exec(
					`insert into pgcat_subscription_progress(subscription, lsn) values($1,$2)
						on conflict(subscription) do update
						set subscription=excluded.subscription, lsn=excluded.lsn`,
					state.sub.Name, &lsn); err != nil {
					state.Panic(err)
				}
				// note that the lsn here is ReorderBufferTXN.end_lsn
				if _, err := state.applyTx.Exec("select pgcat_replication_origin_xact_setup($1,$2)",
					&lsn, v.Timestamp); err != nil {
					state.Panic(err)
				}

				if err := state.applyTx.Commit(); err != nil {
					state.Panic(err)
				}
				state.applyTx = nil

				row := state.applyConn.QueryRow("select pg_current_wal_insert_lsn()")
				var localLSN pgLSN
				if err := row.Scan(&localLSN); err != nil {
					state.Panic(err)
				}

				// append the info
				applyInfo := new(applyInfo)
				applyInfo.remoteLSN = v.TransactionLSN
				applyInfo.localLSN = uint64(localLSN)

				state.applyInfoList.prev.next = applyInfo
				applyInfo.prev = state.applyInfoList.prev
				state.applyInfoList.prev = applyInfo
				applyInfo.next = state.applyInfoList
			case pgoutput.Insert, pgoutput.Update, pgoutput.Delete:
				if startLSN < state.startPos {
					state.Warnf("skip dml due to progress advanced, confirm_lsn=%s, lsn=%s",
						pgx.FormatLSN(state.startPos), pgx.FormatLSN(startLSN))
					continue
				}
				state.handleDML(&msg)
			case pgoutput.Truncate:
				if startLSN < state.startPos {
					state.Warnf("skip truncate due to progress advanced, confirm_lsn=%s, lsn=%s",
						pgx.FormatLSN(state.startPos), pgx.FormatLSN(startLSN))
					continue
				}
				state.Debugf("msg type=Truncate %+v", v)
				state.handleTruncate(&v)
			default:
				state.Debugf("msg type=unknown %+v", v)
			}
		}

		if heartbeat := repMsg.ServerHeartbeat; heartbeat != nil {
			state.Debugw("Got heartbeat", "heartbeat", heartbeat)
			endLSN := heartbeat.ServerWalEnd
			if state.lastRecvPos < endLSN {
				state.lastRecvPos = endLSN
			}
			state.sendFeedback(false)

			if state.isSync && state.lastRecvPos >= state.endPos {
				state.syncDone()
				return
			}
		}
	}
}

func runSubscription(
	sub *subscription,
	applyConnConfig pgx.ConnConfig,
	maxSyncWorkers int,
	logger *zap.SugaredLogger) *applyState {
	state := &applyState{
		sub:             sub,
		applyConnConfig: applyConnConfig,
		applyCh:         make(chan interface{}),
		closeCh:         make(chan struct{}),
		stopCh:          make(chan struct{}),
		SugaredLogger: logger.With(
			"db", applyConnConfig.Database,
			"sub", sub.Name,
			"sub_host", sub.Hostname,
			"sub_port", sub.Port,
			"sub_pub", sub.Publications,
		),
		maxSyncWorkers: maxSyncWorkers,
	}
	go func() {
		defer func() {
			if err := recover(); err != nil {
				state.Errorf("subscription panic: %+v: %s", err, string(debug.Stack()))
			}
			state.stopTime = time.Now()
			close(state.stopCh)
		}()
		state.run()
	}()
	return state
}

type DbRunState struct {
	*zap.SugaredLogger
	PgxLogLevel       pgx.LogLevel
	Host              string
	Port              uint16
	Password          string
	DB                string
	MaxSyncWorkers    int
	CloseCh           chan struct{}
	StopCh            chan struct{}
	ClientMinMessages string
}

func RunDatabase(state *DbRunState) {
	defer close(state.StopCh)

	state.Infow("run database")

	dialer := proxy.FromEnvironment()
	applyConnConfig := pgx.ConnConfig{
		Host:     state.Host,
		Port:     state.Port,
		Database: state.DB,
		User:     "pgcat",
		Password: state.Password,
		RuntimeParams: map[string]string{
			"application_name": "pgcat",
		},
		Dial: func(network, addr string) (net.Conn, error) {
			return dialer.Dial(network, addr)
		},
		Logger:   zapadapter.NewLogger(zap.L()),
		LogLevel: state.PgxLogLevel,
	}

	if state.ClientMinMessages != "" {
		applyConnConfig.RuntimeParams["client_min_messages"] = state.ClientMinMessages
	}

	conn, err := pgx.Connect(applyConnConfig)
	if err != nil {
		state.Panicw("connect failed", "err", err)
	}

	if err := conn.Listen("pgcat_cfg_changed"); err != nil {
		state.Panic(err)
	}

	// run all subscriptions
	tx, err := conn.Begin()
	if err != nil {
		state.Panic(err)
	}

	subscriptions, err := querySubscriptions(tx)
	if err != nil {
		state.Panic(err)
	}

	err = tx.Commit()
	if err != nil {
		state.Panic(err)
	}

	states := make(map[string]*applyState)
	defer func() {
		for _, st := range states {
			close(st.closeCh)
			<-st.stopCh
		}
	}()

	for _, sub := range subscriptions {
		if sub.Enabled {
			states[sub.Name] = runSubscription(
				sub, applyConnConfig, state.MaxSyncWorkers, state.SugaredLogger)
		}
	}

	// take care of configuration changes and restart failed run
	for {
		ctx, cancelFn := context.WithTimeout(context.Background(), 3*time.Second)
		notify, err := conn.WaitForNotification(ctx)
		cancelFn()

		select {
		case <-state.CloseCh:
			return
		default:
		}

		if err != nil {
			if err != context.DeadlineExceeded {
				state.Panic(err)
			}

			// keep conn alive
			conn.Ping(context.Background())

			// restart all failed run
			for _, st := range states {
				select {
				case <-st.stopCh:
					if time.Now().Sub(st.stopTime) >= 1*time.Minute {
						st.Warn("restart failed subscription")
						states[st.sub.Name] = runSubscription(
							st.sub, applyConnConfig, state.MaxSyncWorkers, state.SugaredLogger)
					}
				default:
				}
			}
		} else {
			state.Infof("notify: %+v", notify)
			tmp := strings.Split(notify.Payload, " ")
			cmd, subName := tmp[0], tmp[1]

			// stop previous subscription, no matter any dml
			if st, ok := states[subName]; ok {
				delete(states, subName)
				close(st.closeCh)
				<-st.stopCh
			}

			switch cmd {
			case "INSERT", "UPDATE":
				tx, err := conn.Begin()
				if err != nil {
					state.Panic(err)
				}

				sub, err := querySubscription(tx, subName)
				if err != nil {
					state.Panic(err)
				}

				err = tx.Commit()
				if err != nil {
					state.Panic(err)
				}

				if sub.Enabled {
					states[sub.Name] = runSubscription(
						sub, applyConnConfig, state.MaxSyncWorkers, state.SugaredLogger)
				}
			}
		}
	}
}
