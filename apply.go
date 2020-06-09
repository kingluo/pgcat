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

	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/log/zapadapter"
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
		v, err := pglogrepl.ParseLSN(val)
		if err != nil {
			return err
		}
		*lsn = pgLSN(v)
	default:
		return fmt.Errorf("cannot decode as pg_lsn, src=%+v", src)
	}

	return nil
}

func formatLSN(lsn uint64) string {
	return pglogrepl.LSN(lsn).String()
}

func (lsn *pgLSN) Value() (driver.Value, error) {
	return formatLSN(uint64(*lsn)), nil
}

func (state *applyState) sendFeedback(replyRequested bool) {
	var flushLSN pgLSN
	row := state.applyConn.QueryRow(context.Background(), "select pg_current_wal_flush_lsn()")
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

	k := pglogrepl.StandbyStatusUpdate{
		WALFlushPosition: pglogrepl.LSN(flushPos),
		WALApplyPosition: pglogrepl.LSN(writePos),
		WALWritePosition: pglogrepl.LSN(state.lastRecvPos),
		ReplyRequested:   replyRequested}

	if err := pglogrepl.SendStandbyStatusUpdate(context.Background(), state.repConn.PgConn(), k); err != nil {
		state.Panicf("SendStandbyStatusUpdate: %s", err)
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
	repConn    *pgx.Conn
	applyTx    pgx.Tx
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
	tx, _ := state.applyConn.Begin(context.Background())
	originName := state.sub.Name

	if state.isSync {
		originName = fmt.Sprintf("%s_sync_%d", state.sub.Name, state.relation.ID)
	}

	row := tx.QueryRow(context.Background(), "select pgcat_replication_origin_oid($1) IS NULL", originName)
	var originNotExist bool
	if err := row.Scan(&originNotExist); err != nil {
		return errors.WithMessagef(err, "check origin failed, origin=%s", originName)
	}

	if originNotExist {
		var oid pgtype.OID
		row2 := tx.QueryRow(context.Background(), "select pgcat_replication_origin_create($1)", originName)
		if err := row2.Scan(&oid); err != nil {
			return errors.WithMessagef(err, "create origin failed, origin=%s", originName)
		}
	}

	state.originName = originName

	_, err := tx.Exec(context.Background(), "select pgcat_replication_origin_session_setup($1)", originName)
	if err != nil {
		return errors.WithMessagef(err, "setup session origin, origin=%s", originName)
	}

	// register lsn type
	typ := pgtype.DataType{Value: &pgtype.Int8{}, Name: "lsn", OID: 3220}
	tx.Conn().ConnInfo().RegisterDataType(typ)

	if !state.isSync {
		var startLsn pgLSN
		row := tx.QueryRow(context.Background(), "select lsn from pgcat_subscription_progress where subscription=$1", state.sub.Name)
		if err := row.Scan(&startLsn); err != nil && err != pgx.ErrNoRows {
			return err
		}
		state.startPos = uint64(startLsn)
		state.Infof("start from lsn: %d", startLsn)
	}

	return tx.Commit(context.Background())
}

const pgErrorDuplicateObject = "42710"
const pgErrorInFailedSQLTransaction = "25P02"

func (state *applyState) run() {
	state.insertTemplate = template.Must(template.New("insertSql").Parse(insertSQL))
	state.updateTemplate = template.Must(template.New("updateSql").Parse(updateSQL))
	state.deleteTemplate = template.Must(template.New("deleteSql").Parse(deleteSQL))

	state.Debugw("start subscription")

	// log notice
	state.applyConnConfig.OnNotice = func(conn *pgconn.PgConn, notice *pgconn.Notice) {
		state.Warnf("NOTICE: %+v", notice)
	}

	// create apply connection and setup origin
	var err error
	state.applyConn, err = pgx.ConnectConfig(context.Background(), &state.applyConnConfig)
	if err != nil {
		state.Panicw("connect failed", "err", err)
	}
	defer state.applyConn.Close(context.Background())

	if _, err := state.applyConn.Exec(context.Background(),
		"select pgcat_set_session_replication_role()"); err != nil {
		state.Panic(err)
	}

	if err := state.setupOrigin(); err != nil {
		state.Panicw("setup apply conn failed", "err", err)
	}

	defer func() {
		if state.isSync {
			_, err := state.applyConn.Exec(context.Background(),
				`select pgcat_replication_origin_session_reset()`)
			if err != nil {
				pgErr := err.(*pgconn.PgError)
				if pgErr.Code == pgErrorInFailedSQLTransaction {
					state.Warnw("pgcat_replication_origin_session_reset failed",
						"err", err)
				} else {
					state.Panicw("pgcat_replication_origin_session_reset failed",
						"err", err)
				}
			} else {
				_, err = state.applyConn.Exec(context.Background(),
					`select pgcat_replication_origin_drop($1)`, state.originName)
				if err != nil {
					state.Panicw("remove temporary origin failed",
						"origin", state.originName, "err", err)
				}
			}
		}
	}()

	// start replication

	repConnConfig, _ := pgx.ParseConfig("")
	state.repConnConfig = *repConnConfig
	cfg2, _ := pgconn.ParseConfig("")
	cfg2.Fallbacks = nil
	cfg2.Host = state.sub.Hostname
	cfg2.Port = state.sub.Port
	cfg2.Database = state.sub.Dbname
	cfg2.User = state.sub.Username
	cfg2.Password = state.sub.Password
	cfg2.RuntimeParams = map[string]string{
		"application_name": "pgcat",
		"replication":      "database",
	}
	cfg2.DialFunc = state.applyConnConfig.DialFunc
	cfg2.OnNotice = func(conn *pgconn.PgConn, notice *pgconn.Notice) {
		state.Warnf("replication NOTICE: %+v", notice)
	}
	state.repConnConfig.Config = *cfg2
	state.repConnConfig.Logger = zapadapter.NewLogger(zap.L())
	state.repConnConfig.LogLevel = state.applyConnConfig.LogLevel
	state.repConnConfig.PreferSimpleProtocol = true

	state.repConn, err = pgx.ConnectConfig(context.Background(), &state.repConnConfig)
	if err != nil {
		state.Panic(err)
	}
	defer state.repConn.Close(context.Background())

	typ := pgtype.DataType{Value: &pgtype.TextArray{}, Name: "_text", OID: 1009}

	state.repConn.ConnInfo().RegisterDataType(typ)

	state.applyInfoList = new(applyInfo)
	state.applyInfoList.prev = state.applyInfoList
	state.applyInfoList.next = state.applyInfoList

	if state.isSync {
		if state.copyTable() {
			return
		}
	} else {
		state.slotName = state.sub.Name
		_, err = pglogrepl.CreateReplicationSlot(
			context.Background(),
			state.repConn.PgConn(),
			state.slotName,
			"pgcat",
			pglogrepl.CreateReplicationSlotOptions{
				Temporary:      false,
				SnapshotAction: "NOEXPORT_SNAPSHOT",
				Mode:           pglogrepl.LogicalReplication,
			})
		if err != nil {
			pgErr := err.(*pgconn.PgError)
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
	pluginArguments := []string{
		"proto_version '1'",
		fmt.Sprintf(`publication_names '%s'`, strings.Join(state.sub.Publications, ",")),
	}
	opts := pglogrepl.StartReplicationOptions{
		Timeline:   -1,
		Mode:       pglogrepl.LogicalReplication,
		PluginArgs: pluginArguments,
	}
	if err := pglogrepl.StartReplication(
		context.Background(),
		state.repConn.PgConn(),
		state.slotName,
		pglogrepl.LSN(state.startPos),
		opts); err != nil {
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
	repMsgCh := make(chan *pgproto3.CopyData, 100)

	// we do copy above, to get lastest lsn immediately,
	// we send empty standby status to request heartbeat.
	if state.isSync {
		state.sendFeedback(true)
	}
	isKeepAliveCh := make(chan struct{})

	repWg.Add(1)
	go func() {
		defer repWg.Done()
		defer close(repMsgCh)
		for {
			msg, err := state.repConn.PgConn().ReceiveMessage(ctx)

			select {
			case <-ctx.Done():
				return
			default:
			}

			if err != nil {
				state.Panicf("%+v %+v", err, state)
			}

			if msg == nil {
				state.Warnw("empty msg")
				continue
			}

			switch msg := msg.(type) {
			case *pgproto3.CopyData:
				isKeepAlive := msg.Data[0] == pglogrepl.PrimaryKeepaliveMessageByteID

				msg2 := *msg
				select {
				case repMsgCh <- &msg2:
				case <-repCloseCh:
					return
				}

				if isKeepAlive {
					select {
					case <-isKeepAliveCh:
					case <-repCloseCh:
						return
					}
				}
			default:
				state.Panicf("Received unexpected message: %#v\n", msg)
			}
		}
	}()

	refreshPubTicker := time.NewTicker(3 * time.Minute)
	defer refreshPubTicker.Stop()

	for {
		var repMsg *pgproto3.CopyData

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

		if repMsg.Data[0] == pglogrepl.XLogDataByteID {
			walMsg, err := pglogrepl.ParseXLogData(repMsg.Data[1:])
			if err != nil {
				state.Errorf("ParseXLogData %+v", err)
			}

			startLSN := uint64(walMsg.WALStart)
			endLSN := uint64(walMsg.ServerWALEnd)
			if state.lastRecvPos < startLSN {
				state.lastRecvPos = startLSN
			}
			if state.lastRecvPos < endLSN {
				state.lastRecvPos = endLSN
			}

			msg, err := pgoutput.Parse(walMsg.WALData)
			if err != nil {
				state.Panic(err)
			}

			switch v := msg.(type) {
			case pgoutput.Relation:
				state.Debugf("msg type=Relation %+v", v)
				state.handleRelation(v)
			case pgoutput.Begin:
				state.commitLsn = v.LSN
				state.Debugf("msg type=Begin, lsn=%d %+v", walMsg.WALStart, v)
				if state.isSync {
					continue
				}

				state.applyTx, err = state.applyConn.Begin(context.Background())
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
				if _, err := state.applyTx.Exec(context.Background(),
					`insert into pgcat_subscription_progress(subscription, lsn) values($1,$2)
						on conflict(subscription) do update
						set subscription=excluded.subscription, lsn=excluded.lsn`,
					state.sub.Name, &lsn); err != nil {
					state.Panic(err)
				}
				// note that the lsn here is ReorderBufferTXN.end_lsn
				if _, err := state.applyTx.Exec(context.Background(), "select pgcat_replication_origin_xact_setup($1,$2)",
					&lsn, v.Timestamp); err != nil {
					state.Panic(err)
				}

				if err := state.applyTx.Commit(context.Background()); err != nil {
					state.Panic(err)
				}
				state.applyTx = nil

				row := state.applyConn.QueryRow(context.Background(), "select pg_current_wal_insert_lsn()")
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
						formatLSN(state.startPos), formatLSN(startLSN))
					continue
				}
				state.handleDML(&msg)
			case pgoutput.Truncate:
				if startLSN < state.startPos {
					state.Warnf("skip truncate due to progress advanced, confirm_lsn=%s, lsn=%s",
						formatLSN(state.startPos), formatLSN(startLSN))
					continue
				}
				state.Debugf("msg type=Truncate %+v", v)
				state.handleTruncate(&v)
			default:
				state.Debugf("msg type=unknown %+v", v)
			}
		}

		if repMsg.Data[0] == pglogrepl.PrimaryKeepaliveMessageByteID {
			heartbeat, err := pglogrepl.ParsePrimaryKeepaliveMessage(repMsg.Data[1:])
			if err != nil {
				state.Errorf("ParsePrimaryKeepaliveMessage %+v", err)
			}
			state.Debugw("Got heartbeat", "heartbeat", heartbeat)
			endLSN := uint64(heartbeat.ServerWALEnd)
			if state.lastRecvPos < endLSN {
				state.lastRecvPos = endLSN
			}
			state.sendFeedback(false)
			isKeepAliveCh <- struct{}{}

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

// DbRunState contains the state of RunDatabase
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

// RunDatabase runs all subscribers of the database
func RunDatabase(state *DbRunState) {
	defer close(state.StopCh)

	state.Infow("run database")

	dialer := proxy.FromEnvironment()

	cfg1, _ := pgx.ParseConfig("")
	applyConnConfig := *cfg1
	cfg2, _ := pgconn.ParseConfig("")
	cfg2.Fallbacks = nil
	cfg2.Host = state.Host
	cfg2.Port = state.Port
	cfg2.Database = state.DB
	cfg2.User = "pgcat"
	cfg2.Password = state.Password
	cfg2.RuntimeParams = map[string]string{
		"application_name": "pgcat",
	}
	cfg2.DialFunc = func(ctx context.Context, network, addr string) (net.Conn, error) {
		return dialer.Dial(network, addr)
	}
	applyConnConfig.Config = *cfg2
	applyConnConfig.Logger = zapadapter.NewLogger(zap.L())
	applyConnConfig.LogLevel = state.PgxLogLevel

	if state.ClientMinMessages != "" {
		applyConnConfig.RuntimeParams["client_min_messages"] = state.ClientMinMessages
	}
	conn, err := pgx.ConnectConfig(context.Background(), &applyConnConfig)
	if err != nil {
		state.Panicw("connect failed", "err", err)
	}

	if _, err := conn.Exec(context.Background(), "listen pgcat_cfg_changed"); err != nil {
		state.Panic(err)
	}

	// run all subscriptions
	tx, err := conn.Begin(context.Background())
	if err != nil {
		state.Panic(err)
	}

	subscriptions, err := querySubscriptions(tx)
	if err != nil {
		state.Panic(err)
	}

	err = tx.Commit(context.Background())
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
			select {
			case <-ctx.Done():
			default:
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
				tx, err := conn.Begin(context.Background())
				if err != nil {
					state.Panic(err)
				}

				sub, err := querySubscription(tx, subName)
				if err != nil {
					state.Panic(err)
				}

				err = tx.Commit(context.Background())
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
