package pgcat

import (
	"context"
	"fmt"
	"io"
	"runtime/debug"
	"strings"
	"time"

	"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgtype"
	"github.com/kyleconroy/pgoutput"
	"github.com/pkg/errors"
)

type copyHandler struct {
	buf     []byte
	ch      chan []byte
	closeCh chan struct{}
}

func (handler *copyHandler) Read(p []byte) (n int, err error) {
	if len(handler.buf) > 0 {
		n = copy(p, handler.buf)
		handler.buf = handler.buf[n:]
	} else {
		data, ok := <-handler.ch
		if !ok {
			return 0, io.EOF
		}
		if len(data) > len(p) {
			handler.buf = data[len(p):]
			data = data[:len(p)]
		}
		n = copy(p, data)
	}
	return
}

func (handler *copyHandler) Write(p []byte) (n int, err error) {
	select {
	case <-handler.closeCh:
		return 0, errors.New("closed")
	default:
	}
	handler.ch <- p
	return len(p), nil
}

const (
	relStateSyncNone = iota
	relStateSyncStart
	relStateSyncCatchUp
	relStateSyncDone
	relStateSyncFailed
)

func (state *applyState) syncDone() {
	state.updateRelState("r", state.relation)

	if err := state.applyTx.Commit(); err != nil {
		state.Fatal(err)
	}

	state.relation.state = relStateSyncDone
	select {
	case state.applyCh <- state:
		state.Infow("sync done")
	case <-state.closeCh:
	}
	return
}

func (state *applyState) updateRelState(st string, relState *relationState) {
	fullName := fmt.Sprintf("%s.%s", relState.Namespace, relState.Name)
	if _, err := state.applyTx.Exec(`insert into pgcat_subscription_rel(subscription,remotetable,localtable,state)
values($1,$2,$3,$4) on conflict(subscription, remotetable, localtable) do update set state=excluded.state`,
		state.sub.Name, fullName, relState.localFullName, st); err != nil {
		state.Fatal(err)
	}
}

func (state *applyState) getOrInsertRelState(relState *relationState) {
	fullName := fmt.Sprintf("%s.%s", relState.Namespace, relState.Name)
	var st string
	row := state.applyConn.QueryRow(`select state from pgcat_subscription_rel
					where subscription=$1 and remotetable=$2 and localtable=$3`,
		state.sub.Name, fullName, relState.localFullName)
	if err := row.Scan(&st); err != nil {
		if err == pgx.ErrNoRows {
			if _, err := state.applyConn.Exec(`insert into pgcat_subscription_rel
(subscription, remotetable, localtable, state) values($1,$2,$3,'i')`,
				state.sub.Name, fullName, relState.localFullName); err != nil {
				state.Fatal(err)
			}
		} else {
			state.Fatal(err)
		}
	} else if st == "r" {
		relState.state = relStateSyncDone
	}
}

func (state *applyState) populateRelations() {
	if state.relations == nil {
		state.relations = make(map[uint32]*relationState)
	}
	pubs := make([]string, len(state.sub.Publications))
	for i, pub := range state.sub.Publications {
		pubs[i] = fmt.Sprintf("'%s'", pub)
	}
	pubList := strings.Join(pubs, ",")
	const getTablesSQL = `select attrelid,schemaname,tablename,(array_agg(attname)::text[])
from pg_publication_tables, pg_attribute
where pubname in (%s) and (schemaname || '.' || tablename)::regclass=attrelid
and attnum >0 and attisdropped=false group by attrelid,schemaname,tablename`
	tmpConn, err := pgx.ReplicationConnect(state.repConnConfig)
	if err != nil {
		state.Panic(err)
	}
	defer tmpConn.Close()

	typ := pgtype.DataType{Value: &pgtype.TextArray{}, Name: "_text", OID: 1009}

	tmpConn.ConnInfo.RegisterDataType(typ)

	rows, err := tmpConn.Query(fmt.Sprintf(getTablesSQL, pubList))
	if err != nil {
		state.Panic(err)
	}
	defer rows.Close()
	for rows.Next() {
		rel := pgoutput.Relation{}
		var cols []string
		err := rows.Scan(
			&rel.ID,
			&rel.Namespace,
			&rel.Name,
			&cols,
		)
		if err != nil {
			state.Panic(err)
		}

		if _, ok := state.relations[rel.ID]; ok {
			continue
		}

		for _, col := range cols {
			rel.Columns = append(rel.Columns, pgoutput.Column{Name: col})
		}
		relState := &relationState{Relation: rel}
		state.mapTableName(state.sub, relState)
		state.getOrInsertRelState(relState)
		state.Infof("add new table, publications=%s, remote_table=%s, local_table=%s",
			pubList, rel.Namespace+"."+rel.Name, relState.localFullName)
		state.relations[rel.ID] = relState
	}
	if err := rows.Err(); err != nil {
		state.Panic(err)
	}
}

func (state *applyState) copyTable() bool {
	// start local transaction
	var err error
	state.applyTx, err = state.applyConn.Begin()
	if err != nil {
		state.Fatal(err)
	}

	//start remote transaction
	txOpts := &pgx.TxOptions{IsoLevel: "REPEATABLE READ", AccessMode: "READ ONLY"}
	repTx, err := state.repConn.BeginEx(context.Background(), txOpts)
	if err != nil {
		state.Fatal(err)
	}

	// create temporary slot and use current transaction snapshot
	state.slotName = fmt.Sprintf("%s_sync_%d", state.sub.Name, state.relation.ID)
	_, err = state.repConn.Exec(fmt.Sprintf(
		"CREATE_REPLICATION_SLOT %s TEMPORARY LOGICAL %s USE_SNAPSHOT", state.slotName, "pgcat"))
	if err != nil {
		state.Fatal(err)
	}

	// lock and refresh table def both sides
	// NOTE:
	// local lock hold during the whole table sync,
	// in the same way the apply process does.
	// However, remote lock hold only during copy command, because
	// the replication stream starting from the slot snapshot would
	// send relation update to us if any.
	if _, err := repTx.Exec(fmt.Sprintf("LOCK TABLE %s.%s IN ACCESS SHARE MODE",
		state.relation.Namespace, state.relation.Name)); err != nil {
		state.Fatal(err)
	}
	row := repTx.QueryRow(fmt.Sprintf(`select array_agg(attname)::text[] from pg_attribute
		where attrelid=%d and attnum >0 and attisdropped=false`, state.relation.ID))
	rel := state.relation.Relation
	// clear the columns
	rel.Columns = rel.Columns[:0]
	var cols []string
	if err := row.Scan(&cols); err != nil {
		state.Fatal(err)
	}
	for _, col := range cols {
		rel.Columns = append(rel.Columns, pgoutput.Column{Name: col})
	}
	state.relation.Relation = rel
	state.localTable.remoteInSync = false
	state.localTable.localInSync = false
	doRelMap(state.localTable, state.relation, state.applyConn)

	// pipe the copy
	state.Infow("start copy",
		"schema", state.relation.Namespace,
		"table", state.relation.Name)
	state.updateRelState("d", state.relation)

	copyHandler := &copyHandler{ch: make(chan []byte), closeCh: state.closeCh}

	cols2 := make([]string, len(state.localTable.Columns))
	for i, col := range state.localTable.Columns {
		cols2[i] = col.Name
	}
	colList := strings.Join(cols2, ",")
	go func() {
		_, err := repTx.CopyToWriter(copyHandler,
			fmt.Sprintf("COPY %s.%s (%s) TO STDOUT",
				state.relation.Namespace, state.relation.Name, colList))
		if err != nil {
			state.Panic(err)
		}
		close(copyHandler.ch)
	}()

	cpy, err := state.applyTx.CopyFromReader(copyHandler,
		fmt.Sprintf("COPY %s (%s) FROM STDIN",
			state.relation.localFullName, colList))
	if err != nil {
		state.Panicw("COPY FROM failed", "err", err)
	}
	state.Infow("copy done", "result", cpy)

	// commit the remote transaction
	repTx.Commit()

	// notify apply goroutine to wait for catching up
	state.relation.state = relStateSyncCatchUp
	select {
	case state.applyCh <- state:
	case <-state.closeCh:
		return true
	}

	select {
	case tmp := <-state.syncCh:
		state.endPos = tmp.(uint64)
	case <-state.closeCh:
		return true
	}

	state.Infow("start catchup", "startPos", state.startPos, "endPos", state.endPos)
	state.updateRelState("c", state.relation)

	// finish and return if sync is caught up with the apply
	if state.startPos >= state.endPos {
		state.syncDone()
		return true
	}

	return false
}

func (state *applyState) startTableSync() {
	for _, rel := range state.relations {
		if rel.state == relStateSyncNone ||
			(rel.state == relStateSyncFailed &&
				(time.Now().Sub(rel.failedTime) >= 1*time.Minute)) {
			localTable, err := getLocalTable(state.sub, rel, state.localTables, state.applyConn)
			if err != nil {
				state.Fatal(err)
			}

			if len(state.syncWorkers) >= state.maxSyncWorkers {
				break
			}

			isFailed := (rel.state == relStateSyncFailed)

			rel.state = relStateSyncStart
			// make copy of pointer fields
			relation2 := *rel
			localTable2 := *localTable
			st := &applyState{
				sub:             state.sub,
				applyConnConfig: state.applyConnConfig,
				isSync:          true,
				relation:        &relation2,
				localTable:      &localTable2,
				syncCh:          make(chan interface{}),
				applyCh:         state.applyCh,
				closeCh:         make(chan struct{}),
				startPos:        state.lastRecvPos,
				SugaredLogger: state.With(
					"isSync", true,
					"relation", fmt.Sprintf("%s.%s", rel.Namespace, rel.Name),
					"localTable", localTable.Name,
				),
			}
			state.syncWorkers[rel.ID] = st
			state.syncWg.Add(1)
			go func() {
				defer func() {
					state.syncWg.Done()
					if r := recover(); r != nil {
						st.relation.state = relStateSyncFailed
						st.relation.failedTime = time.Now()
						select {
						case state.applyCh <- st:
						case <-state.closeCh:
						}
						st.Errorf("sync failed: %+v: %s", r, string(debug.Stack()))
					}
				}()
				if isFailed {
					st.Warn("restart failed subscription")
				}
				st.run()
			}()
		}
	}
}
