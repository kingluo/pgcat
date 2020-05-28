module github.com/kingluo/pgcat

go 1.12

require (
	github.com/jackc/fake v0.0.0-20150926172116-812a484cc733 // indirect
	github.com/jackc/pgconn v1.5.0
	github.com/jackc/pglogrepl v0.0.0-20200309144228-32ec418076b3
	github.com/jackc/pgproto3/v2 v2.0.1
	github.com/jackc/pgtype v1.3.0
	github.com/jackc/pgx/v4 v4.6.0
	github.com/kyleconroy/pgoutput v0.1.1-0.20181230214841-6f49f4f3563f
	github.com/lib/pq v1.2.0
	github.com/pkg/errors v0.8.1
	go.uber.org/atomic v1.6.0 // indirect
	go.uber.org/multierr v1.1.0 // indirect
	go.uber.org/zap v1.10.0
	golang.org/x/net v0.0.0-20190813141303-74dc4d7220e7
	gopkg.in/yaml.v2 v2.2.2
)

replace github.com/kyleconroy/pgoutput => github.com/kingluo/pgoutput v0.1.1-0.20190621062920-5086ff3db43e
