package timescaledb

import (
	"go.k6.io/k6/output"
	"go.k6.io/k6/stats"
)

func init() {
	output.RegisterExtension("timescaledb", func(params output.Params) (output.Output, error) {
		return &Output{}, nil
	})
}

type Output struct {
	Config config
}

func (o *Output) Description() string {
	return "Output to TimescaleDB"
}

func (o *Output) Start() error {
	return nil
}

func (o *Output) AddMetricSamples(samples []stats.SampleContainer) {}

func (o *Output) Stop() error { return nil }
