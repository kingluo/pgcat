package pgcat

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/kyleconroy/pgoutput"
	"github.com/pkg/errors"
)

type relationState struct {
	pgoutput.Relation
	state          int
	localNamespace string
	localName      string
	localFullName  string
	failedTime     time.Time
}

type localColumn struct {
	Name        string
	IsReplIdent bool
	RemoteIdx   int
}

type localTableState struct {
	Name         string
	Columns      []localColumn
	localInSync  bool
	remoteInSync bool
}

func (state *applyState) mapTableName(sub *subscription, relation *relationState) {
	// map the name
	if relation.localFullName == "" {
		fullName := fmt.Sprintf("%s.%s", relation.Namespace, relation.Name)
		var mapping tableMapping
		for _, mapping = range sub.mapping {
			if fullName == mapping.src {
				relation.localFullName = mapping.dst
				state.Debugf("use direct mapping, %s -> %s", fullName, relation.localFullName)
				break
			} else if mapping.regexp.MatchString(fullName) {
				relation.localFullName = mapping.regexp.ReplaceAllString(fullName, mapping.dst)
				state.Debugf("use regexp mapping, %s -> %s", fullName, relation.localFullName)
				break
			}
		}
		if relation.localFullName != "" {
			tmp := strings.Split(relation.localFullName, ".")
			if len(tmp) != 2 {
				state.Panic("invalid table mapping result, mapping=%s, from=%s, to=%s",
					mapping, fullName, relation.localFullName)
			}
			relation.localNamespace = tmp[0]
			relation.localName = tmp[1]
		} else {
			relation.localFullName = fullName
			relation.localNamespace = relation.Namespace
			relation.localName = relation.Name
		}
	}
}

func doRelMap(localTable *localTableState, relation *relationState, applyConn *pgx.Conn) error {
	if !localTable.localInSync {
		var inSync bool
		row := applyConn.QueryRow(context.Background(), "select pgcat_check_table($1, $2)",
			relation.localNamespace, relation.localName)
		if err := row.Scan(&inSync); err != nil {
			return errors.Wrap(err, "pgcat_check_table failed")
		}

		if !inSync {
			rows, err := applyConn.Query(context.Background(), "select * from pgcat_get_table_columns($1, $2)",
				relation.localNamespace, relation.localName)
			if err != nil {
				return err
			}
			defer rows.Close()

			localTable.Columns = localTable.Columns[:0]
			for rows.Next() {
				col := localColumn{}
				err := rows.Scan(&col.Name, &col.IsReplIdent)
				if err != nil {
					return err
				}
				localTable.Columns = append(localTable.Columns, col)
			}

			if err := rows.Err(); err != nil {
				return err
			}

			localTable.remoteInSync = false
		}

		localTable.localInSync = true
	}

	// remove missing columns at remote side
	if !localTable.remoteInSync {
		for i := 0; i < len(localTable.Columns); {
			col := &localTable.Columns[i]
			remoteIdx := -1
			for j, c := range relation.Columns {
				if c.Name == col.Name {
					remoteIdx = j
					break
				}
			}
			if remoteIdx == -1 {
				if col.IsReplIdent {
					return fmt.Errorf(
						"missing replication identity at remote side, table=%s, column=%s",
						relation.localFullName, col.Name)
				}
				localTable.Columns = append(localTable.Columns[:i], localTable.Columns[i+1:]...)
			} else {
				col.RemoteIdx = remoteIdx
				i++
			}
		}
		localTable.remoteInSync = true
	}

	return nil
}

func getLocalTable(sub *subscription, relation *relationState,
	localTables map[string]*localTableState, applyConn *pgx.Conn) (*localTableState, error) {
	localTable, ok := localTables[relation.localFullName]
	if ok && localTable.localInSync && localTable.remoteInSync {
		return localTable, nil
	}

	if !ok {
		localTable = &localTableState{Name: relation.localFullName}
		localTables[relation.localFullName] = localTable
	}

	if err := doRelMap(localTable, relation, applyConn); err != nil {
		return nil, err
	}

	return localTable, nil
}

func (state *applyState) handleRelation(v pgoutput.Relation) {
	if state.isSync {
		if state.relation.ID == v.ID {
			// update the relation info
			state.relation.Relation = v
			state.localTable.remoteInSync = false
			doRelMap(state.localTable, state.relation, state.applyConn)
		}
		return
	}

	var relState *relationState
	if oldRelState, ok := state.relations[v.ID]; ok {
		relState = oldRelState
		// update the relation info
		relState.Relation = v
	} else {
		relState = &relationState{Relation: v}
		state.mapTableName(state.sub, relState)
		state.updateRelState("i", relState)
		state.relations[v.ID] = relState
	}

	// invalidate relation binding
	if t, ok := state.localTables[relState.localFullName]; ok {
		t.remoteInSync = false
	}
}
