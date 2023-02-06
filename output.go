// Package timescaledb provides the xk6-output-timescaledb extension
package timescaledb

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirupsen/logrus"

	"go.k6.io/k6/metrics"
	"go.k6.io/k6/output"
)

func init() {
	output.RegisterExtension("timescaledb", newOutput)
}

var _ interface{ output.WithThresholds } = &Output{}

// Output is a k6 output that sends metrics to a TimescaleDB instance.
type Output struct {
	output.SampleBuffer
	periodicFlusher *output.PeriodicFlusher
	Pool            *pgxpool.Pool
	Config          config

	thresholds map[string][]*dbThreshold

	logger logrus.FieldLogger
}

// Description returns a short human-readable description of the output.
func (o *Output) Description() string {
	return fmt.Sprintf("TimescaleDB (%s)", o.Config.addr.String)
}

func newOutput(params output.Params) (output.Output, error) {
	configs, err := getConsolidatedConfig(params.JSONConfig, params.Environment, params.ConfigArgument)
	if err != nil {
		return nil, fmt.Errorf("problem parsing config: %w", err)
	}

	pconf, err := pgxpool.ParseConfig(configs.URL.String)
	if err != nil {
		return nil, fmt.Errorf("TimescaleDB: Unable to parse config: %w", err)
	}

	pool, err := pgxpool.NewWithConfig(context.Background(), pconf)
	if err != nil {
		return nil, fmt.Errorf("TimescaleDB: Unable to create connection pool: %w", err)
	}

	o := Output{
		Pool:   pool,
		Config: configs,
		logger: params.Logger.WithFields(logrus.Fields{
			"output": "TimescaleDB",
		}),
	}

	return &o, nil
}

// SetThresholds receives threshold data to be output.
func (o *Output) SetThresholds(thresholds map[string]metrics.Thresholds) {
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
	threshold *metrics.Threshold
}

const schema = `
	CREATE TABLE IF NOT EXISTS samples (
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

// Start initializes the output.
func (o *Output) Start() error {
	conn, err := o.Pool.Acquire(context.Background())
	if err != nil {
		o.logger.WithError(err).Error("Start: Couldn't acquire connection")
	}
	defer conn.Release()

	_, err = conn.Exec(context.Background(), "CREATE DATABASE "+o.Config.dbName.String)
	if err != nil {
		o.logger.WithError(err).Debug("Start: Couldn't create database; most likely harmless")
	}
	_, err = conn.Exec(context.Background(), schema)
	if err != nil {
		o.logger.WithError(err).Debug("Start: Couldn't create database schema; most likely harmless")
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
				o.logger.WithError(err).Debug("Start: Failed to insert threshold")
			}
		}
	}

	pf, err := output.NewPeriodicFlusher(time.Duration(o.Config.PushInterval.Duration), o.flushMetrics)
	if err != nil {
		return err
	}

	o.logger.Debug("Start: Running!")
	o.periodicFlusher = pf

	return nil
}

func (o *Output) flushMetrics() {
	sampleContainers := o.GetBufferedSamples()
	start := time.Now()

	o.logger.WithField("sample-containers", len(sampleContainers)).Debug("flushMetrics: Collecting...")

	rows := [][]interface{}{}
	for _, sc := range sampleContainers {
		samples := sc.GetSamples()
		o.logger.WithField("samples", len(samples)).Debug("flushMetrics: Writing...")

		for _, s := range samples {
			tags := s.Tags.Map()
			row := []interface{}{s.Time, s.Metric.Name, s.Value, tags}
			rows = append(rows, row)
		}
	}

	var batch pgx.Batch
	for _, t := range o.thresholds {
		for _, threshold := range t {
			batch.Queue(`UPDATE thresholds SET last_failed = $1 WHERE id = $2`,
				threshold.threshold.LastFailed, threshold.id)
		}
	}

	br := o.Pool.SendBatch(context.Background(), &batch)
	defer func() {
		if err := br.Close(); err != nil {
			o.logger.WithError(err).Warn("flushMetrics: Couldn't close batch results")
		}
	}()

	_, err := o.Pool.CopyFrom(context.Background(),
		pgx.Identifier{"samples"},
		[]string{"ts", "metric", "value", "tags"},
		pgx.CopyFromRows(rows))
	if err != nil {
		o.logger.WithError(err).Warn("copyMetrics: Couldn't commit samples")
	}

	for i := 0; i < batch.Len(); i++ {
		ct, err := br.Exec()
		if err != nil {
			o.logger.WithError(err).Error("flushMetrics: Couldn't exec batch")
			return
		}
		if ct.RowsAffected() != 1 {
			o.logger.WithError(err).Error("flushMetrics: Batch did not insert")
			return
		}
	}

	t := time.Since(start)
	o.logger.WithField("time_since_start", t).Debug("flushMetrics: Samples committed!")
}

// Stop stops the output.
func (o *Output) Stop() error {
	o.logger.Debug("Stopping...")
	defer o.logger.Debug("Stopped!")
	o.periodicFlusher.Stop()
	o.Pool.Close()
	return nil
}
