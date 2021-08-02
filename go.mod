module github.com/shirleyleu/xk6-output-timescaledb

go 1.16

require (
	github.com/jackc/fake v0.0.0-20150926172116-812a484cc733 // indirect
	github.com/jackc/pgx v3.6.2+incompatible
	github.com/jackc/pgx/v4 v4.13.0
	github.com/sirupsen/logrus v1.8.1
	go.k6.io/k6 v0.33.0
	gopkg.in/guregu/null.v3 v3.5.0
)

replace go.k6.io/k6 => /Users/shirley/go/src/github.com/grafana/k6
