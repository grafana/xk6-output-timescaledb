package timescaledb

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"

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

func parseURL(text string) (config, error) {
	u, err := url.Parse(text)
	if err != nil {
		return config{}, err
	}
	var c config
	c.URL = null.NewString(u.String(), true)
	if u.Host != "" {
		c.addr = null.StringFrom(u.Scheme + "://" + u.Host)
	}
	if db := strings.TrimPrefix(u.Path, "/"); db != "" {
		c.db = null.StringFrom(db)
	}
	for k, vs := range u.Query() {
		switch k {
		case "pushInterval":
			if err := c.PushInterval.UnmarshalText([]byte(vs[0])); err != nil {
				return config{}, err
			}
		case "concurrentWrites":
			writes, err := strconv.Atoi(vs[0])
			if err != nil {
				return c, err
			}
			c.ConcurrentWrites = null.IntFrom(int64(writes))
		default:
			return config{}, fmt.Errorf("unknown query parameter: %s", k)
		}
	}
	return c, err
}
