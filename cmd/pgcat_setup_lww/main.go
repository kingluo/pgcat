package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"text/template"

	"golang.org/x/net/proxy"

	"github.com/jackc/pgx"
	"gopkg.in/yaml.v2"
)

type tableData struct {
	Table          string
	TableName      string
	Row            []string
	Identity       []string
	NonIdentCols   []string
	InlineCols     []string
	IndividualCols []string
	CounterCols    []string
}

type lwwTableCfg struct {
	Name           string   `yaml:"name"`
	IndividualCols []string `yaml:"IndividualCols"`
	CounterCols    []string `yaml:"CounterCols"`
}

type lwwDBCfg struct {
	Name   string         `yaml:"name"`
	Tables []*lwwTableCfg `yaml:"tables"`
}

type lwwCfg struct {
	Host      string      `yaml:"host"`
	Port      uint16      `yaml:"port"`
	User      string      `yaml:"user"`
	Password  string      `yaml:"password"`
	Databases []*lwwDBCfg `yaml:"databases"`
}

var populateTemplate *template.Template
var lwwTemplate *template.Template

func execTemplate(t *template.Template, data *tableData, conn *pgx.Conn, dryRun bool) {
	var buf bytes.Buffer
	t.Execute(&buf, data)
	sql := buf.String()
	if !dryRun {
		_, err := conn.Exec(sql)
		if err != nil {
			if pgErr, ok := err.(pgx.PgError); ok {
				log.Fatalf("%#v", pgErr)
			} else {
				log.Fatal(err)
			}
			fmt.Println(sql)
		}
	} else {
		fmt.Println(sql)
	}
}

func setupTable(cfg *lwwTableCfg, row []string, ident []string, conn *pgx.Conn, dryRun bool) {
	var nonIdentCols []string
	var inlineCols []string
outloop:
	for _, col := range row {
		for _, col2 := range ident {
			if col == col2 {
				continue outloop
			}
		}

		nonIdentCols = append(nonIdentCols, col)

		for _, col2 := range cfg.IndividualCols {
			if col == col2 {
				continue outloop
			}
		}

		for _, col2 := range cfg.CounterCols {
			if col == col2 {
				continue outloop
			}
		}

		inlineCols = append(inlineCols, col)
	}

	data := &tableData{
		Table:          cfg.Name,
		TableName:      cfg.Name,
		Row:            row,
		Identity:       ident,
		NonIdentCols:   nonIdentCols,
		InlineCols:     inlineCols,
		IndividualCols: cfg.IndividualCols,
		CounterCols:    cfg.CounterCols,
	}

	tmp := strings.Split(cfg.Name, ".")
	if len(tmp) == 2 {
		data.TableName = tmp[1]
	}

	log.Printf("setup table: %+v", data)

	needPopulate := true
	for _, col := range row {
		if col == "__pgcat_lww" {
			needPopulate = false
			break
		}
	}

	log.Println("--> setup triggers, view, index")
	execTemplate(lwwTemplate, data, conn, dryRun)

	if needPopulate {
		log.Println("--> populate __pgcat_lww")
		execTemplate(populateTemplate, data, conn, dryRun)
	}
}

var isMap = map[byte]string{
	'd': "indisprimary",
	'i': "indisreplident",
}

const getColsSQLStr = `
select attname::text, ARRAY[attnum] <@ indkey from pg_attribute,
	(SELECT indrelid, string_to_array(indkey::text, ' ')::int2[] as indkey
		FROM pg_index WHERE indrelid = $1::regclass and %s=true) as t
	where indrelid = attrelid and attnum > 0 and attisdropped=false order by attnum;
`

//go:generate bash generate_sql_template.sh

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	cfgFilePath := flag.String("c", "lww.yml", "config file")
	dryRun := flag.Bool("dryrun", false, "dry run, only output rendered sql")
	flag.Parse()

	populateTemplate = template.Must(template.New("populateSQL").Parse(populateSQL))
	lwwTemplate = template.Must(template.New("lwwSQL").Parse(lwwSQL))

	cfgFile, err := os.Open(*cfgFilePath)
	defer cfgFile.Close()
	if err != nil {
		log.Fatal(err)
	}

	decoder := yaml.NewDecoder(cfgFile)
	dialer := proxy.FromEnvironment()

	for {
		lwwCfg := &lwwCfg{}
		var err error
		if err = decoder.Decode(lwwCfg); err != nil && err != io.EOF {
			log.Fatal(err)
		}
		if err == io.EOF {
			break
		}
		log.Printf("%+v", lwwCfg)
		for _, db := range lwwCfg.Databases {
			connCfg := pgx.ConnConfig{
				Host:     lwwCfg.Host,
				Port:     lwwCfg.Port,
				Database: db.Name,
				User:     lwwCfg.User,
				Password: lwwCfg.Password,
				RuntimeParams: map[string]string{
					"application_name": "pgcat",
				},
				Dial: func(network, addr string) (net.Conn, error) {
					return dialer.Dial(network, addr)
				},
			}
			conn, err := pgx.Connect(connCfg)
			if err != nil {
				log.Fatal(err)
			}

			for _, table := range db.Tables {
				var kind byte
				var ident byte
				row := conn.QueryRow(`select relkind, relreplident from pg_class
					where oid = $1::regclass`, table.Name)
				err := row.Scan(&kind, &ident)
				if err != nil {
					log.Fatal(err)
				}
				if kind != 'r' || (ident != 'd' && ident != 'i') {
					log.Fatal("must be base table with pk or unique index as replica ident")
				}
				rows, err := conn.Query(fmt.Sprintf(getColsSQLStr, isMap[ident]), table.Name)
				if err != nil {
					log.Fatal(err)
				}
				defer rows.Close()

				var cols []string
				var identCols []string
				for rows.Next() {
					var col string
					var isIdent bool
					err := rows.Scan(&col, &isIdent)
					if err != nil {
						log.Fatal(err)
					}
					if col == "__pgcat_lww" {
						continue
					}

					cols = append(cols, col)
					if isIdent {
						identCols = append(identCols, col)
					}
				}
				if err := rows.Err(); err != nil {
					log.Fatal(err)
				}

				setupTable(table, cols, identCols, conn, *dryRun)
			}
		}
	}
}
