package prometheus

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/codewandler/clstr-go/core/cluster"
	"github.com/codewandler/clstr-go/core/metrics"
)

// clusterMetrics implements cluster.ClusterMetrics using Prometheus.
type clusterMetrics struct {
	requestDuration *prometheus.HistogramVec
	requestsTotal   *prometheus.CounterVec
	notifiesTotal   *prometheus.CounterVec
	transportErrors *prometheus.CounterVec
	handlerDuration *prometheus.HistogramVec
	handlersTotal   *prometheus.CounterVec
	handlersActive  *prometheus.GaugeVec
	shardsOwned     *prometheus.GaugeVec
}

// NewClusterMetrics creates a new Prometheus implementation of ClusterMetrics.
func NewClusterMetrics(reg prometheus.Registerer) cluster.ClusterMetrics {
	m := &clusterMetrics{
		requestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "clstr_cluster_request_duration_seconds",
			Help:    "Client request latency in seconds",
			Buckets: defaultBuckets,
		}, []string{"message_type"}),

		requestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "clstr_cluster_requests_total",
			Help: "Total number of client requests",
		}, []string{"message_type", "success"}),

		notifiesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "clstr_cluster_notifies_total",
			Help: "Total number of client notifications",
		}, []string{"message_type", "success"}),

		transportErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "clstr_cluster_transport_errors_total",
			Help: "Total number of transport errors",
		}, []string{"error_type"}),

		handlerDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "clstr_cluster_handler_duration_seconds",
			Help:    "Handler execution time in seconds",
			Buckets: defaultBuckets,
		}, []string{"message_type"}),

		handlersTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "clstr_cluster_handlers_total",
			Help: "Total number of handlers executed",
		}, []string{"message_type", "success"}),

		handlersActive: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "clstr_cluster_handlers_active",
			Help: "Number of concurrent handlers",
		}, []string{"node_id"}),

		shardsOwned: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "clstr_cluster_shards_owned",
			Help: "Number of shards owned by node",
		}, []string{"node_id"}),
	}

	reg.MustRegister(
		m.requestDuration,
		m.requestsTotal,
		m.notifiesTotal,
		m.transportErrors,
		m.handlerDuration,
		m.handlersTotal,
		m.handlersActive,
		m.shardsOwned,
	)

	return m
}

func (m *clusterMetrics) RequestDuration(msgType string) metrics.Timer {
	return newTimer(m.requestDuration.WithLabelValues(msgType))
}

func (m *clusterMetrics) RequestCompleted(msgType string, success bool) {
	m.requestsTotal.WithLabelValues(msgType, boolToStr(success)).Inc()
}

func (m *clusterMetrics) NotifyCompleted(msgType string, success bool) {
	m.notifiesTotal.WithLabelValues(msgType, boolToStr(success)).Inc()
}

func (m *clusterMetrics) TransportError(errorType string) {
	m.transportErrors.WithLabelValues(errorType).Inc()
}

func (m *clusterMetrics) HandlerDuration(msgType string) metrics.Timer {
	return newTimer(m.handlerDuration.WithLabelValues(msgType))
}

func (m *clusterMetrics) HandlerCompleted(msgType string, success bool) {
	m.handlersTotal.WithLabelValues(msgType, boolToStr(success)).Inc()
}

func (m *clusterMetrics) HandlersActive(nodeID string, count int) {
	m.handlersActive.WithLabelValues(nodeID).Set(float64(count))
}

func (m *clusterMetrics) ShardsOwned(nodeID string, count int) {
	m.shardsOwned.WithLabelValues(nodeID).Set(float64(count))
}

var _ cluster.ClusterMetrics = (*clusterMetrics)(nil)
