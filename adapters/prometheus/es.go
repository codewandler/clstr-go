package prometheus

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/codewandler/clstr-go/core/es"
	"github.com/codewandler/clstr-go/core/metrics"
)

// esMetrics implements es.ESMetrics using Prometheus.
type esMetrics struct {
	// Store metrics
	storeLoadDuration   *prometheus.HistogramVec
	storeAppendDuration *prometheus.HistogramVec
	eventsAppended      *prometheus.CounterVec

	// Repository metrics
	repoLoadDuration     *prometheus.HistogramVec
	repoSaveDuration     *prometheus.HistogramVec
	concurrencyConflicts *prometheus.CounterVec

	// Cache metrics
	cacheHits   *prometheus.CounterVec
	cacheMisses *prometheus.CounterVec

	// Snapshot metrics
	snapshotLoadDuration *prometheus.HistogramVec
	snapshotSaveDuration *prometheus.HistogramVec

	// Consumer metrics
	consumerEventDuration *prometheus.HistogramVec
	consumerEvents        *prometheus.CounterVec
	consumerLag           *prometheus.GaugeVec
}

// NewESMetrics creates a new Prometheus implementation of ESMetrics.
func NewESMetrics(reg prometheus.Registerer) es.ESMetrics {
	m := &esMetrics{
		storeLoadDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "clstr_es_store_load_duration_seconds",
			Help:    "Event store load latency in seconds",
			Buckets: defaultBuckets,
		}, []string{"aggregate_type"}),

		storeAppendDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "clstr_es_store_append_duration_seconds",
			Help:    "Event store append latency in seconds",
			Buckets: defaultBuckets,
		}, []string{"aggregate_type"}),

		eventsAppended: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "clstr_es_events_appended_total",
			Help: "Total number of events appended",
		}, []string{"aggregate_type"}),

		repoLoadDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "clstr_es_repo_load_duration_seconds",
			Help:    "Repository load latency in seconds",
			Buckets: defaultBuckets,
		}, []string{"aggregate_type"}),

		repoSaveDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "clstr_es_repo_save_duration_seconds",
			Help:    "Repository save latency in seconds",
			Buckets: defaultBuckets,
		}, []string{"aggregate_type"}),

		concurrencyConflicts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "clstr_es_concurrency_conflicts_total",
			Help: "Total number of optimistic lock failures",
		}, []string{"aggregate_type"}),

		cacheHits: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "clstr_es_cache_hits_total",
			Help: "Total number of cache hits",
		}, []string{"aggregate_type"}),

		cacheMisses: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "clstr_es_cache_misses_total",
			Help: "Total number of cache misses",
		}, []string{"aggregate_type"}),

		snapshotLoadDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "clstr_es_snapshot_load_duration_seconds",
			Help:    "Snapshot load latency in seconds",
			Buckets: defaultBuckets,
		}, []string{"aggregate_type"}),

		snapshotSaveDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "clstr_es_snapshot_save_duration_seconds",
			Help:    "Snapshot save latency in seconds",
			Buckets: defaultBuckets,
		}, []string{"aggregate_type"}),

		consumerEventDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "clstr_es_consumer_event_duration_seconds",
			Help:    "Event processing time in seconds",
			Buckets: defaultBuckets,
		}, []string{"event_type", "live"}),

		consumerEvents: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "clstr_es_consumer_events_total",
			Help: "Total number of events processed",
		}, []string{"event_type", "live", "success"}),

		consumerLag: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "clstr_es_consumer_lag",
			Help: "Consumer lag (sequences behind)",
		}, []string{"consumer"}),
	}

	reg.MustRegister(
		m.storeLoadDuration,
		m.storeAppendDuration,
		m.eventsAppended,
		m.repoLoadDuration,
		m.repoSaveDuration,
		m.concurrencyConflicts,
		m.cacheHits,
		m.cacheMisses,
		m.snapshotLoadDuration,
		m.snapshotSaveDuration,
		m.consumerEventDuration,
		m.consumerEvents,
		m.consumerLag,
	)

	return m
}

func (m *esMetrics) StoreLoadDuration(aggType string) metrics.Timer {
	return newTimer(m.storeLoadDuration.WithLabelValues(aggType))
}

func (m *esMetrics) StoreAppendDuration(aggType string) metrics.Timer {
	return newTimer(m.storeAppendDuration.WithLabelValues(aggType))
}

func (m *esMetrics) EventsAppended(aggType string, count int) {
	m.eventsAppended.WithLabelValues(aggType).Add(float64(count))
}

func (m *esMetrics) RepoLoadDuration(aggType string) metrics.Timer {
	return newTimer(m.repoLoadDuration.WithLabelValues(aggType))
}

func (m *esMetrics) RepoSaveDuration(aggType string) metrics.Timer {
	return newTimer(m.repoSaveDuration.WithLabelValues(aggType))
}

func (m *esMetrics) ConcurrencyConflict(aggType string) {
	m.concurrencyConflicts.WithLabelValues(aggType).Inc()
}

func (m *esMetrics) CacheHit(aggType string) {
	m.cacheHits.WithLabelValues(aggType).Inc()
}

func (m *esMetrics) CacheMiss(aggType string) {
	m.cacheMisses.WithLabelValues(aggType).Inc()
}

func (m *esMetrics) SnapshotLoadDuration(aggType string) metrics.Timer {
	return newTimer(m.snapshotLoadDuration.WithLabelValues(aggType))
}

func (m *esMetrics) SnapshotSaveDuration(aggType string) metrics.Timer {
	return newTimer(m.snapshotSaveDuration.WithLabelValues(aggType))
}

func boolToStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

func (m *esMetrics) ConsumerEventDuration(eventType string, live bool) metrics.Timer {
	return newTimer(m.consumerEventDuration.WithLabelValues(eventType, boolToStr(live)))
}

func (m *esMetrics) ConsumerEventProcessed(eventType string, live bool, success bool) {
	m.consumerEvents.WithLabelValues(eventType, boolToStr(live), boolToStr(success)).Inc()
}

func (m *esMetrics) ConsumerLag(consumer string, lag int64) {
	m.consumerLag.WithLabelValues(consumer).Set(float64(lag))
}

var _ es.ESMetrics = (*esMetrics)(nil)
