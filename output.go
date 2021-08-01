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

const expectedDatabaseSchema = `CREATE TABLE IF NOT EXISTS samples (
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
	conn, err := o.Pool.Acquire()
	if err != nil {
		o.logger.WithError(err).Error("TimescaleDB: Couldn't acquire connection")
	}
	_, err = conn.Exec("CREATE DATABASE " + o.Config.db.String)
	if err != nil {
		o.logger.WithError(err).Debug("TimescaleDB: Couldn't create database; most likely harmless")
	}
	_, err = conn.Exec(expectedDatabaseSchema)
	if err != nil {
		o.logger.WithError(err).Debug("TimescaleDB: Couldn't create database schema; most likely harmless")
	}

	for name, t := range o.thresholds {
		for _, threshold := range t {
			metric, _, tags := stats.ParseThresholdName(name)
			err = conn.QueryRow("INSERT INTO thresholds (metric, tags, threshold, abort_on_fail, delay_abort_eval, last_failed) VALUES ($1, $2, $3, $4, $5, $6) RETURNING id",
				metric, tags, threshold.threshold.Source, threshold.threshold.AbortOnFail, threshold.threshold.AbortGracePeriod.String(), threshold.threshold.LastFailed).Scan(&threshold.id)
			if err != nil {
				o.logger.WithError(err).Debug("TimescaleDB: Failed to insert threshold")
			}
		}
	}

	o.Pool.Release(conn)
	return nil
}

func (o *Output) AddMetricSamples(samples []stats.SampleContainer) {}

func (o *Output) Stop() error { return nil }
