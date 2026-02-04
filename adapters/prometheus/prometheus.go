// Package prometheus provides Prometheus implementations of the metrics interfaces
// for all three pillars (ES, Actor, Cluster).
package prometheus

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/codewandler/clstr-go/core/metrics"
)

// timer wraps a Prometheus histogram to implement the Timer interface.
type timer struct {
	h     prometheus.Observer
	start time.Time
}

func newTimer(h prometheus.Observer) metrics.Timer {
	return &timer{h: h, start: time.Now()}
}

func (t *timer) ObserveDuration() {
	t.h.Observe(time.Since(t.start).Seconds())
}

// Default histogram buckets for latency metrics (in seconds).
var defaultBuckets = []float64{
	.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10,
}

// AllMetrics holds Prometheus implementations for all three pillars.
// Use this when you want to initialize metrics for your entire application at once.
type AllMetrics struct {
	ES      *esMetrics
	Actor   *actorMetrics
	Cluster *clusterMetrics
}

// NewAllMetrics creates Prometheus metrics for all three pillars.
// This is the recommended way to set up metrics when using all pillars together.
func NewAllMetrics(reg prometheus.Registerer) *AllMetrics {
	return &AllMetrics{
		ES:      NewESMetrics(reg).(*esMetrics),
		Actor:   NewActorMetrics(reg).(*actorMetrics),
		Cluster: NewClusterMetrics(reg).(*clusterMetrics),
	}
}
