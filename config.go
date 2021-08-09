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
	PgUrl        null.String        `json:"pg_url"`
	PushInterval types.NullDuration `json:"push_interval"`
	dbName       null.String
	addr         null.String
}

func newConfig() config {
	return config{
		PgUrl:        null.NewString("postgresql://localhost/myk6timescaleDB", false),
		PushInterval: types.NewNullDuration(time.Second, false),
		dbName:       null.NewString("myk6timescaleDB", false),
		addr:         null.NewString("postgresql://localhost", false),
	}
}

func (c config) apply(modifiedConf config) config {
	if modifiedConf.PgUrl.Valid {
		c.PgUrl = modifiedConf.PgUrl
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
		var rawJsonConf config
		if err := json.Unmarshal(jsonRawConf, &rawJsonConf); err != nil {
			return config{}, fmt.Errorf("problem unmarshalling json: %w", err)
		}
		consolidatedConf = consolidatedConf.apply(rawJsonConf)

		jsonUrlConf, err := parseUrl(consolidatedConf.PgUrl.String)
		if err != nil {
			return config{}, fmt.Errorf("problem parsing url provided in json %q: %w",
				consolidatedConf.PgUrl.String, err)
		}
		consolidatedConf = consolidatedConf.apply(jsonUrlConf)
	}

	envPgUrl, ok := env["K6_TIMESCALEDB_URL"]
	if ok {
		envUrlConf, err := parseUrl(envPgUrl)
		if err != nil {
			return config{}, fmt.Errorf("invalid K6_TIMESCALEDB_URL %q: %w", envPgUrl, err)
		}
		consolidatedConf = consolidatedConf.apply(envUrlConf)
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
		parsedConfArg, err := parseUrl(confArg)
		if err != nil {
			return config{}, fmt.Errorf("invalid config argument %q: %w", confArg, err)
		}
		consolidatedConf = consolidatedConf.apply(parsedConfArg)
	}

	return consolidatedConf, nil
}

func parseUrl(text string) (config, error) {
	u, err := url.Parse(text)
	if err != nil {
		return config{}, err
	}
	var parsedConf config
	parsedConf.PgUrl = null.StringFrom(u.Scheme + "://" + u.User.String() + "@" + u.Host + u.Path)

	if u.Host != "" {
		parsedConf.addr = null.StringFrom(u.Scheme + "://" + u.Host)
	}
	if dbName := strings.TrimPrefix(u.Path, "/"); dbName != "" {
		parsedConf.dbName = null.StringFrom(dbName)
	}
	for k, vs := range u.Query() {
		switch k {
		case "push_interval":
			if err := parsedConf.PushInterval.UnmarshalText([]byte(vs[0])); err != nil {
				return config{}, err
			}
		default:
			return config{}, fmt.Errorf("unknown query parameter: %s", k)
		}
	}
	return parsedConf, err
}
