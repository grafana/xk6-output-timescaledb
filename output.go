package timescaledb

import "github.com/grafana/k6/stats"

type Output struct{}

func (o *Output) Description() string{
	return ""
}

func (o *Output) Start() error{
	return nil
}

func (o *Output) AddMetricSamples(samples []stats.SampleContainer) {}

func (o *Output) Stop() error {return nil}