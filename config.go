package timescaledb

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type config struct {
	// Connection URL in the form specified in the libpq docs,
	// see https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING):
	// postgresql://[user[:password]@][netloc][:port][,...][/dbname][?param1=value1&...]
	URL              string        `json:"addr" envconfig:"K6_TIMESCALEDB_URL"`
	PushInterval     time.Duration `json:"pushInterval,omitempty" envconfig:"K6_TIMESCALEDB_PUSH_INTERVAL"`
	ConcurrentWrites int           `json:"concurrentWrites,omitempty" envconfig:"K6_TIMESCALEDB_CONCURRENT_WRITES"`
}

func getEnvConfig() (config, error) {
	pushInterval, err := time.ParseDuration(os.Getenv("K6_TIMESCALEDB_PUSH_INTERVAL"))
	if err != nil {
		return config{}, fmt.Errorf("invalid K6_TIMESCALEDB_PUSH_INTERVAL: %w", err)
	}
	concurrentWrites, err := strconv.Atoi(os.Getenv("K6_TIMESCALEDB_CONCURRENT_WRITES"))
	if err != nil {
		return config{}, fmt.Errorf("invalid K6_TIMESCALEDB_CONCURRENT_WRITES: %w", err)
	}

	return config{
		URL:              os.Getenv("K6_TIMESCALEDB_URL"),
		PushInterval:     pushInterval,
		ConcurrentWrites: concurrentWrites,
	}, nil
}
