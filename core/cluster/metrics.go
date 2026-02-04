package cluster

import "github.com/codewandler/clstr-go/core/metrics"

// ClusterMetrics defines the metrics interface for the Cluster pillar.
// All methods are thread-safe.
type ClusterMetrics interface {
	// Client operations
	RequestDuration(msgType string) metrics.Timer
	RequestCompleted(msgType string, success bool)
	NotifyCompleted(msgType string, success bool)

	// Transport errors: no_subscriber, timeout, ttl_expired
	TransportError(errorType string)

	// Handler operations
	HandlerDuration(msgType string) metrics.Timer
	HandlerCompleted(msgType string, success bool)
	HandlersActive(nodeID string, count int)

	// Shards
	ShardsOwned(nodeID string, count int)
}

// nopClusterMetrics is a no-op implementation of ClusterMetrics.
type nopClusterMetrics struct{}

func (nopClusterMetrics) RequestDuration(string) metrics.Timer { return metrics.NopTimer() }
func (nopClusterMetrics) RequestCompleted(string, bool)        {}
func (nopClusterMetrics) NotifyCompleted(string, bool)         {}

func (nopClusterMetrics) TransportError(string) {}

func (nopClusterMetrics) HandlerDuration(string) metrics.Timer { return metrics.NopTimer() }
func (nopClusterMetrics) HandlerCompleted(string, bool)        {}
func (nopClusterMetrics) HandlersActive(string, int)           {}

func (nopClusterMetrics) ShardsOwned(string, int) {}

// NopClusterMetrics returns a no-op ClusterMetrics implementation.
func NopClusterMetrics() ClusterMetrics { return nopClusterMetrics{} }
