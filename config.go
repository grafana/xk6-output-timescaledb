package timescaledb

import (
	"go.k6.io/k6/lib/types"
	"gopkg.in/guregu/null.v3"
)

type config struct {
	// Connection URL in the form specified in the libpq docs,
	// see https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING):
	// postgresql://[user[:password]@][netloc][:port][,...][/dbname][?param1=value1&...]
	URL              null.String        `json:"addr" envconfig:"K6_TIMESCALEDB_URL"`
	PushInterval     types.NullDuration `json:"pushInterval,omitempty" envconfig:"K6_TIMESCALEDB_PUSH_INTERVAL"`
	ConcurrentWrites null.Int           `json:"concurrentWrites,omitempty" envconfig:"K6_TIMESCALEDB_CONCURRENT_WRITES"`

	addr null.String
	db   null.String
}
