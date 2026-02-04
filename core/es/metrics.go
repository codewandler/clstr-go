package es

import "github.com/codewandler/clstr-go/core/metrics"

// ESMetrics defines the metrics interface for the Event Sourcing pillar.
// All methods return Timer or increment counters; implementations should
// be thread-safe.
type ESMetrics interface {
	// Store operations
	StoreLoadDuration(aggType string) metrics.Timer
	StoreAppendDuration(aggType string) metrics.Timer
	EventsAppended(aggType string, count int)

	// Repository operations
	RepoLoadDuration(aggType string) metrics.Timer
	RepoSaveDuration(aggType string) metrics.Timer
	ConcurrencyConflict(aggType string)

	// Cache
	CacheHit(aggType string)
	CacheMiss(aggType string)

	// Snapshots
	SnapshotLoadDuration(aggType string) metrics.Timer
	SnapshotSaveDuration(aggType string) metrics.Timer

	// Consumer
	ConsumerEventDuration(eventType string, live bool) metrics.Timer
	ConsumerEventProcessed(eventType string, live bool, success bool)
	ConsumerLag(consumer string, lag int64)
}

// nopESMetrics is a no-op implementation of ESMetrics.
type nopESMetrics struct{}

func (nopESMetrics) StoreLoadDuration(string) metrics.Timer   { return metrics.NopTimer() }
func (nopESMetrics) StoreAppendDuration(string) metrics.Timer { return metrics.NopTimer() }
func (nopESMetrics) EventsAppended(string, int)               {}

func (nopESMetrics) RepoLoadDuration(string) metrics.Timer { return metrics.NopTimer() }
func (nopESMetrics) RepoSaveDuration(string) metrics.Timer { return metrics.NopTimer() }
func (nopESMetrics) ConcurrencyConflict(string)            {}

func (nopESMetrics) CacheHit(string)  {}
func (nopESMetrics) CacheMiss(string) {}

func (nopESMetrics) SnapshotLoadDuration(string) metrics.Timer { return metrics.NopTimer() }
func (nopESMetrics) SnapshotSaveDuration(string) metrics.Timer { return metrics.NopTimer() }

func (nopESMetrics) ConsumerEventDuration(string, bool) metrics.Timer { return metrics.NopTimer() }
func (nopESMetrics) ConsumerEventProcessed(string, bool, bool)        {}
func (nopESMetrics) ConsumerLag(string, int64)                        {}

// NopESMetrics returns a no-op ESMetrics implementation.
func NopESMetrics() ESMetrics { return nopESMetrics{} }

// ESMetricsOption sets the metrics for ES components.
type ESMetricsOption struct{ m ESMetrics }

// WithMetrics sets the metrics implementation for ES components.
func WithMetrics(m ESMetrics) ESMetricsOption { return ESMetricsOption{m: m} }

func (o ESMetricsOption) applyToEnv(e *envOptions)            { e.metrics = o.m }
func (o ESMetricsOption) applyToRepository(r *repoOpts)       { r.metrics = o.m }
func (o ESMetricsOption) applyToConsumerOpts(c *consumerOpts) { c.metrics = o.m }
