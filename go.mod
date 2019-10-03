module github.com/kingluo/pgcat

go 1.12

require (
	github.com/cockroachdb/apd v1.1.0 // indirect
	github.com/gofrs/uuid v3.2.0+incompatible // indirect
	github.com/jackc/fake v0.0.0-20150926172116-812a484cc733 // indirect
	github.com/jackc/pgx v3.6.1-0.20190927150758-23388fecf653+incompatible
	github.com/kyleconroy/pgoutput v0.1.1-0.20181230214841-6f49f4f3563f
	github.com/lib/pq v1.2.0
	github.com/pkg/errors v0.8.1
	github.com/shopspring/decimal v0.0.0-20180709203117-cd690d0c9e24 // indirect
	github.com/stretchr/testify v1.3.0 // indirect
	go.uber.org/atomic v1.4.0 // indirect
	go.uber.org/multierr v1.1.0 // indirect
	go.uber.org/zap v1.10.0
	golang.org/x/crypto v0.0.0-20190618222545-ea8f1a30c443 // indirect
	golang.org/x/net v0.0.0-20190404232315-eb5bcb51f2a3
	golang.org/x/text v0.3.2 // indirect
	gopkg.in/yaml.v2 v2.2.2
)

replace github.com/kyleconroy/pgoutput => github.com/kingluo/pgoutput v0.1.1-0.20190621062920-5086ff3db43e
