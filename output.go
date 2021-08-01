package timescaledb

import (
	"github.com/jackc/pgx"
	"github.com/sirupsen/logrus"

	"go.k6.io/k6/output"
	"go.k6.io/k6/stats"
)

func init() {
	output.RegisterExtension("timescaledb", func(params output.Params) (output.Output, error) {
		return &Output{}, nil
	})
}

type Output struct {
	Pool   *pgx.ConnPool
	Config config

	thresholds map[string][]*dbThreshold

	logger logrus.FieldLogger
}

func (o *Output) Description() string {
	return "Output to TimescaleDB"
}

type dbThreshold struct {
	id        int
	threshold *stats.Threshold
}

// TimescaleDB database schema expected by this collector
const databaseSchema = `CREATE TABLE IF NOT EXISTS samples (
		ts timestamptz NOT NULL DEFAULT current_timestamp,
		metric varchar(128) NOT NULL,
		tags jsonb,
		value real
	);
	CREATE TABLE IF NOT EXISTS thresholds (
		id serial,
		ts timestamptz NOT NULL DEFAULT current_timestamp,
		metric varchar(128) NOT NULL,
		tags jsonb,
		threshold varchar(128) NOT NULL,
		abort_on_fail boolean DEFAULT FALSE,
		delay_abort_eval varchar(128),
		last_failed boolean DEFAULT FALSE
	);
	SELECT create_hypertable('samples', 'ts');
	CREATE INDEX IF NOT EXISTS idx_samples_ts ON samples (ts DESC);
	CREATE INDEX IF NOT EXISTS idx_thresholds_ts ON thresholds (ts DESC);`

func (o *Output) Start() error {
	return nil
}

func (o *Output) AddMetricSamples(samples []stats.SampleContainer) {}

func (o *Output) Stop() error { return nil }
