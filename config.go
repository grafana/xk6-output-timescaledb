package timescaledb

import (
	"fmt"
	"strconv"
	"time"
)

type config struct {
	// Connection URL in the form specified in the libpq docs,
	// see https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING):
	// postgresql://[user[:password]@][netloc][:port][,...][/dbname][?param1=value1&...]
	URL              string
	PushInterval     time.Duration
	ConcurrentWrites int
}

func getEnvConfig(env map[string]string) (config, error) {
	url, ok := env["K6_TIMESCALEDB_URL"]
	if !ok {
		return config{}, fmt.Errorf("invalid K6_TIMESCALEDB_URL")
	}

	pushInterval, err := time.ParseDuration(env["K6_TIMESCALEDB_PUSH_INTERVAL"])
	if err != nil {
		return config{}, fmt.Errorf("invalid K6_TIMESCALEDB_PUSH_INTERVAL: %w", err)
	}

	concurrentWrites, err := strconv.Atoi(env["K6_TIMESCALEDB_CONCURRENT_WRITES"])
	if err != nil {
		return config{}, fmt.Errorf("invalid K6_TIMESCALEDB_CONCURRENT_WRITES: %w", err)
	}

	return config{
		URL:              url,
		PushInterval:     pushInterval,
		ConcurrentWrites: concurrentWrites,
	}, nil
}
