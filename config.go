package timescaledb

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"go.k6.io/k6/lib/types"

	"gopkg.in/guregu/null.v3"
)

type config struct {
	// Connection URL in the form specified in the libpq docs,
	// see https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING):
	// postgresql://[user[:password]@][netloc][:port][,...][/dbname][?param1=value1&...]
	URL          null.String        `json:"url"`
	PushInterval types.NullDuration `json:"pushInterval"`
	dbName       null.String
	addr         null.String
}

func newConfig() config {
	return config{
		URL:          null.NewString("postgresql://localhost/myk6timescaleDB", false),
		PushInterval: types.NewNullDuration(time.Second, false),
		dbName:       null.NewString("myk6timescaleDB", false),
		addr:         null.NewString("postgresql://localhost", false),
	}
}

func (c config) apply(modifiedConf config) config {
	if modifiedConf.URL.Valid {
		c.URL = modifiedConf.URL
	}
	if modifiedConf.PushInterval.Valid {
		c.PushInterval = modifiedConf.PushInterval
	}
	if modifiedConf.dbName.Valid {
		c.dbName = modifiedConf.dbName
	}
	if modifiedConf.addr.Valid {
		c.addr = modifiedConf.addr
	}
	return c
}

func getConsolidatedConfig(jsonRawConf json.RawMessage, env map[string]string, confArg string) (config, error) {
	consolidatedConf := newConfig()

	if jsonRawConf != nil {
		var jsonConf config
		if err := json.Unmarshal(jsonRawConf, &jsonConf); err != nil {
			return config{}, fmt.Errorf("problem unmarshalling JSON: %w", err)
		}
		consolidatedConf = consolidatedConf.apply(jsonConf)

		jsonURLConf, err := parseURL(consolidatedConf.URL.String)
		if err != nil {
			return config{}, fmt.Errorf("problem parsing URL provided in JSON %q: %w",
				consolidatedConf.URL.String, err)
		}
		consolidatedConf = consolidatedConf.apply(jsonURLConf)
	}

	envPushInterval, ok := env["K6_TIMESCALEDB_PUSH_INTERVAL"]
	if ok {
		pushInterval, err := time.ParseDuration(envPushInterval)
		if err != nil {
			return config{}, fmt.Errorf("invalid K6_TIMESCALEDB_PUSH_INTERVAL: %w", err)
		}
		consolidatedConf = consolidatedConf.apply(config{PushInterval: types.NewNullDuration(pushInterval, true)})
	}

	if confArg != "" {
		parsedConfArg, err := parseURL(confArg)
		if err != nil {
			return config{}, fmt.Errorf("invalid config argument %q: %w", confArg, err)
		}
		consolidatedConf = consolidatedConf.apply(parsedConfArg)
	}

	return consolidatedConf, nil
}

func parseURL(text string) (config, error) {
	u, err := url.Parse(text)
	if err != nil {
		return config{}, err
	}
	var parsedConf config
	parsedConf.URL = null.StringFrom(text)

	if u.Host != "" {
		parsedConf.addr = null.StringFrom(u.Scheme + "://" + u.Host)
	}
	if dbName := strings.TrimPrefix(u.Path, "/"); dbName != "" {
		parsedConf.dbName = null.StringFrom(dbName)
	}
	return parsedConf, err
}
