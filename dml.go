package pgcat

import (
	"bytes"
	"strings"

	"github.com/jackc/pgx"
	"github.com/kyleconroy/pgoutput"
	"github.com/lib/pq"
)

type columnData struct {
	Name  string
	Value []byte
	quote *string
}

func (data *columnData) Quote() string {
	if data.Value == nil {
		return "NULL"
	}
	if data.quote == nil {
		str := pq.QuoteLiteral(string(data.Value))
		data.quote = &str
	}
	return *data.quote
}

type rowData struct {
	Table    string
	Row      []*columnData
	Identity []*columnData
}

func (state *applyState) convertRow(localTable *localTableState,
	row1 []pgoutput.Tuple) []*columnData {
	var row2 []*columnData
	for _, col := range localTable.Columns {
		remoteIdx := col.RemoteIdx
		tuple := row1[remoteIdx]
		if tuple.Flag != 'u' {
			row2 = append(row2, &columnData{
				Name:  col.Name,
				Value: tuple.Value,
			})
		}
	}
	return row2
}

func (state *applyState) convertReplIdent(localTable *localTableState,
	row1 []pgoutput.Tuple) []*columnData {
	var row2 []*columnData
	for _, col := range localTable.Columns {
		if !col.IsReplIdent {
			continue
		}
		remoteIdx := col.RemoteIdx
		tuple := row1[remoteIdx]
		if tuple.Flag == 'u' || tuple.Flag == 'n' {
			state.Fatal("null or unchanged toast field could not be replia identity, col=%+v",
				tuple)
		}
		row2 = append(row2, &columnData{
			Name:  col.Name,
			Value: tuple.Value,
		})
	}
	return row2
}

func (state *applyState) insertRowData(msg *pgoutput.Insert,
	localTable *localTableState) *rowData {
	row := &rowData{Table: localTable.Name}
	row.Row = state.convertRow(localTable, msg.Row)
	return row
}

func (state *applyState) updateRowData(msg *pgoutput.Update,
	localTable *localTableState) *rowData {
	row := &rowData{Table: localTable.Name}
	row.Row = state.convertRow(localTable, msg.Row)
	oldRow := msg.OldRow
	if oldRow == nil {
		oldRow = msg.Row
	}
	row.Identity = state.convertReplIdent(localTable, oldRow)
	return row
}

func (state *applyState) deleteRowData(msg *pgoutput.Delete,
	localTable *localTableState) *rowData {
	row := &rowData{Table: localTable.Name}
	row.Identity = state.convertReplIdent(localTable, msg.Row)
	return row
}

const insertSQL = `insert into {{.Table}}(
{{- range $i,$a := .Row}}{{if gt $i 0 }} , {{end}}{{ $a.Name }}{{end -}}
) values(
{{- range $i,$a := .Row}}{{if gt $i 0 }} , {{end}}{{ $a.Quote }}{{end -}}
)`

const updateSQL = `update {{.Table}} set (
{{- range $i,$a := .Row}}{{if gt $i 0 }} , {{end}}{{ $a.Name }}{{end -}}
) = (
{{- range $i,$a := .Row}}{{if gt $i 0 }} , {{end}}{{ $a.Quote }}{{end -}}
) where {{range $i,$a := .Identity}}{{if gt $i 0 }} and {{end}}
{{- $a.Name }} = {{ $a.Quote }}{{end -}}
`

const deleteSQL = `delete from {{.Table}} where {{range $i,$a := .Identity}}
{{- if gt $i 0 }} and {{end}}{{ $a.Name }} = {{ $a.Quote }}{{end -}}`

func (state *applyState) handleDML(msg *pgoutput.Message) {
	var ID uint32
	switch v := (*msg).(type) {
	case pgoutput.Insert:
		state.Debugf("msg type=Insert %+v", v)
		ID = v.RelationID
	case pgoutput.Update:
		state.Debugf("msg type=Update %+v", v)
		ID = v.RelationID
	case pgoutput.Delete:
		state.Debugf("msg type=Delete %+v", v)
		ID = v.RelationID
	}

	if state.isSync && ID != state.relation.ID {
		return
	}

	var localTable *localTableState
	if state.isSync {
		localTable = state.localTable
	} else {
		relation := state.relations[ID]
		if relation.state != relStateSyncDone {
			return
		}

		var err error
		localTable, err = getLocalTable(state.sub, relation, state.localTables, state.applyConn)
		if err != nil {
			state.Panic(err)
		}
	}

	var tpl bytes.Buffer

	switch v := (*msg).(type) {
	case pgoutput.Insert:
		if err := state.insertTemplate.Execute(&tpl,
			state.insertRowData(&v, localTable)); err != nil {
			state.Fatal(err)
		}
	case pgoutput.Update:
		if err := state.updateTemplate.Execute(&tpl,
			state.updateRowData(&v, localTable)); err != nil {
			state.Fatal(err)
		}
	case pgoutput.Delete:
		if err := state.deleteTemplate.Execute(&tpl,
			state.deleteRowData(&v, localTable)); err != nil {
			state.Fatal(err)
		}
	}
	sql := tpl.String()

	state.Debug(sql)
	_, err := state.applyTx.Exec(sql)
	if err != nil {
		state.Panicf("dml failed, commit_lsn=%s, err=%+v",
			pgx.FormatLSN(state.commitLsn), err)
	}
}

func (state *applyState) handleTruncate(v *pgoutput.Truncate) {
	if state.isSync {
		var found bool
		for _, rel := range v.Relations {
			if rel == state.relation.ID {
				found = true
				break
			}
		}
		if !found {
			return
		}
		v.Relations = []uint32{state.relation.ID}
	} else {
		tmp := make([]uint32, 0, len(v.Relations))
		for _, rel := range v.Relations {
			if state.relations[rel].state == relStateSyncDone {
				tmp = append(tmp, rel)
			}
		}
		if len(tmp) == 0 {
			return
		}
		v.Relations = tmp
	}
	tables := make([]string, len(v.Relations))
	for i, rel := range v.Relations {
		tables[i] = state.relations[rel].localFullName
	}
	sql := "truncate " + strings.Join(tables, ",")
	if v.Option&0x2 == 1 {
		sql += " RESTART IDENTITY"
	}
	if v.Option&0x1 == 1 {
		sql += " CASCADE"
	}

	state.Debug(sql)
	_, err := state.applyTx.Exec(sql)
	if err != nil {
		state.Panicf("truncate failed, commit_lsn=%s, err=%+v",
			pgx.FormatLSN(state.commitLsn), state.commitLsn, err)
	}
}
