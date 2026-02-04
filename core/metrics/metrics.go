// Package metrics provides abstract metrics interfaces that allow pluggable
// instrumentation backends (Prometheus, StatsD, etc.) without coupling the
// core packages to any specific implementation.
package metrics

// Counter is a monotonically increasing metric.
type Counter interface {
	// Inc increments the counter by 1.
	Inc()
	// Add increments the counter by delta. delta must be >= 0.
	Add(delta float64)
}

// Gauge is a metric that can go up and down.
type Gauge interface {
	// Set sets the gauge to value.
	Set(value float64)
	// Inc increments the gauge by 1.
	Inc()
	// Dec decrements the gauge by 1.
	Dec()
	// Add adds delta to the gauge. delta can be negative.
	Add(delta float64)
}

// Histogram samples observations (e.g., request latencies) and counts them
// in configurable buckets.
type Histogram interface {
	// Observe adds a single observation to the histogram.
	Observe(value float64)
}

// Timer measures the duration of an operation. Call ObserveDuration when
// the operation completes to record the elapsed time.
type Timer interface {
	// ObserveDuration records the elapsed time since the timer was created.
	ObserveDuration()
}

// TimerFunc is a function that creates a new Timer. This allows deferred
// timing patterns like: defer metrics.StoreLoadDuration("user").ObserveDuration()
type TimerFunc func() Timer
