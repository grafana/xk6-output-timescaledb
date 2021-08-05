package timescaledb

import (
	"encoding/json"
	"fmt"
	"time"

	"go.k6.io/k6/lib/types"

	"gopkg.in/guregu/null.v3"
)

type config struct {
	// Connection URL in the form specified in the libpq docs,
	// see https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING):
	// postgresql://[user[:password]@][netloc][:port][,...][/dbname][?param1=value1&...]
	URL          null.String
	PushInterval types.NullDuration
}

func newConfig() config {
	return config{
		URL:          null.NewString("postgresql://localhost/myk6timescaleDB", false),
		PushInterval: types.NewNullDuration(time.Second, false),
	}
}

func (c config) apply(cfg config) config {
	if cfg.URL.Valid {
		c.URL = cfg.URL
	}
	if cfg.PushInterval.Valid {
		c.PushInterval = cfg.PushInterval
	}
	return c
}

func getConsolidatedConfig(jsonRawConf json.RawMessage, env map[string]string, url string) (config, error) {
	result := newConfig()

	if jsonRawConf != nil {
		var jsonConf config
		err := json.Unmarshal(jsonRawConf, &jsonConf)
		if err != nil {
			return result, err
		}
		result = result.apply(jsonConf)
	}

	pgUrl, ok := env["K6_TIMESCALEDB_URL"]
	if !ok {
		return config{}, fmt.Errorf("invalid K6_TIMESCALEDB_URL: %q", pgUrl)
	}
	result = result.apply(config{URL: null.StringFrom(pgUrl)})

	pushInterval, err := time.ParseDuration(env["K6_TIMESCALEDB_PUSH_INTERVAL"])
	if err != nil {
		return config{}, fmt.Errorf("invalid K6_TIMESCALEDB_PUSH_INTERVAL: %w", err)
	}
	result = result.apply(config{PushInterval: types.NewNullDuration(pushInterval, true)})

	return result, nil
}
