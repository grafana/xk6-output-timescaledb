package timescaledb

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/sirupsen/logrus"

	"go.k6.io/k6/output"
	"go.k6.io/k6/stats"
)

func init() {
	output.RegisterExtension("timescaledb", newOutput)
}

func newOutput(params output.Params) (output.Output, error) {
	config, err := getConsolidatedConfig(params.JSONConfig, params.Environment, params.ConfigArgument)
	if err != nil {
		return nil, fmt.Errorf("problem parsing config: %w", err)
	}

	pconf, err := pgxpool.ParseConfig(config.URL.String)
	if err != nil {
		return nil, fmt.Errorf("TimescaleDB: Unable to parse config: %w", err)
	}

	pool, err := pgxpool.ConnectConfig(context.Background(), pconf)
	if err != nil {
		return nil, fmt.Errorf("TimescaleDB: Unable to create connection pool: %w", err)
	}

	o := Output{
		Pool:   pool,
		Config: config,
		logger: params.Logger.WithFields(logrus.Fields{
			"output": "TimescaleDB",
		}),
	}

	return &o, nil
}

var _ interface{ output.WithThresholds } = &Output{}

type Output struct {
	output.SampleBuffer
	periodicFlusher *output.PeriodicFlusher
	Pool            *pgxpool.Pool
	Config          config

	thresholds map[string][]*dbThreshold

	logger logrus.FieldLogger
}

func (o *Output) Description() string {
	return "TimescaleDB"
}

// SetThresholds receives the thresholds before the output is Start()-ed.
func (o *Output) SetThresholds(thresholds map[string]stats.Thresholds) {
	ths := make(map[string][]*dbThreshold)
	for metric, fullTh := range thresholds {
		for _, t := range fullTh.Thresholds {
			ths[metric] = append(ths[metric], &dbThreshold{
				id:        -1,
				threshold: t,
			})
		}
	}

	o.thresholds = ths
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
		threshold varchar(128) NOT NULL,
		abort_on_fail boolean DEFAULT FALSE,
		delay_abort_eval varchar(128),
		last_failed boolean DEFAULT FALSE
	);
	SELECT create_hypertable('samples', 'ts');
	CREATE INDEX IF NOT EXISTS idx_samples_ts ON samples (ts DESC);
	CREATE INDEX IF NOT EXISTS idx_thresholds_ts ON thresholds (ts DESC);`

func (o *Output) Start() error {
	conn, err := o.Pool.Acquire(context.Background())
	if err != nil {
		o.logger.WithError(err).Error("TimescaleDB: Couldn't acquire connection")
	}
	defer conn.Release()

	_, err = conn.Exec(context.Background(), "CREATE DATABASE myk6timescaleDB")
	if err != nil {
		o.logger.WithError(err).Debug("TimescaleDB: Couldn't create database; most likely harmless")
	}
	_, err = conn.Exec(context.Background(), expectedDatabaseSchema)
	if err != nil {
		o.logger.WithError(err).Debug("TimescaleDB: Couldn't create database schema; most likely harmless")
	}

	for metric, thresholds := range o.thresholds {
		for _, t := range thresholds {
			err = conn.QueryRow(context.Background(), `
				INSERT INTO thresholds (metric, threshold, abort_on_fail, delay_abort_eval, last_failed)
				VALUES ($1, $2, $3, $4, $5)
				RETURNING id`,
				metric, t.threshold.Source, t.threshold.AbortOnFail, t.threshold.AbortGracePeriod.String(), t.threshold.LastFailed).
				Scan(&t.id)
			if err != nil {
				o.logger.WithError(err).Debug("TimescaleDB: Failed to insert threshold")
			}
		}
	}

	pf, err := output.NewPeriodicFlusher(time.Duration(o.Config.PushInterval.Duration), o.commit)
	if err != nil {
		return err
	}

	o.logger.Debug("TimescaleDB: Running!")
	o.periodicFlusher = pf

	return nil
}

func (o *Output) commit() {
	sampleContainers := o.GetBufferedSamples()
	for _, sc := range sampleContainers {
		samples := sc.GetSamples()
		logrus.Debug("TimescaleDB: Committing...")
		o.logger.WithField("samples", len(samples)).Debug("TimescaleDB: Writing...")

		start := time.Now()
		batch := &pgx.Batch{}
		for _, s := range samples {
			tags := s.Tags.CloneTags()
			batch.Queue("INSERT INTO samples (ts, metric, value, tags) VALUES ($1, $2, $3, $4)",
				s.Time, s.Metric.Name, s.Value, tags)
		}

		for _, t := range o.thresholds {
			for _, threshold := range t {
				batch.Queue("UPDATE thresholds SET last_failed = $1 WHERE id = $2",
					[]interface{}{threshold.threshold.LastFailed, threshold.id},
					[]pgtype.OID{pgtype.BoolOID, pgtype.Int4OID},
					nil)
			}
		}

		conn, err := o.Pool.Acquire(context.Background())
		if err != nil {
			logrus.WithError(err).Error("TimescaleDB: Couldn't acquire connection to write samples")
			return
		}
		defer conn.Release()

		br := conn.SendBatch(context.Background(), batch)
		if _, err := br.Exec(); err != nil {
			o.logger.WithError(err).Error("TimescaleDB: Couldn't write samples and update thresholds")
		}

		t := time.Since(start)
		o.logger.WithField("t", t).Debug("TimescaleDB: Batch written!")
	}
}

func (o *Output) Stop() error {
	o.logger.Debug("Stopping...")
	defer o.logger.Debug("Stopped!")
	o.periodicFlusher.Stop()
	o.Pool.Close()
	return nil
}
