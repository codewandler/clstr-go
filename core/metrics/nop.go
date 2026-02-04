package metrics

// nopCounter is a no-op implementation of Counter.
type nopCounter struct{}

func (nopCounter) Inc()        {}
func (nopCounter) Add(float64) {}

// nopGauge is a no-op implementation of Gauge.
type nopGauge struct{}

func (nopGauge) Set(float64) {}
func (nopGauge) Inc()        {}
func (nopGauge) Dec()        {}
func (nopGauge) Add(float64) {}

// nopHistogram is a no-op implementation of Histogram.
type nopHistogram struct{}

func (nopHistogram) Observe(float64) {}

// nopTimer is a no-op implementation of Timer.
type nopTimer struct{}

func (nopTimer) ObserveDuration() {}

// NopCounter returns a no-op Counter.
func NopCounter() Counter { return nopCounter{} }

// NopGauge returns a no-op Gauge.
func NopGauge() Gauge { return nopGauge{} }

// NopHistogram returns a no-op Histogram.
func NopHistogram() Histogram { return nopHistogram{} }

// NopTimer returns a no-op Timer.
func NopTimer() Timer { return nopTimer{} }

// NopTimerFunc returns a TimerFunc that always returns a no-op Timer.
func NopTimerFunc() TimerFunc { return func() Timer { return nopTimer{} } }
