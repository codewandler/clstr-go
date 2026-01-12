package actor

import (
	"log/slog"
	"sync/atomic"
)

type scheduleFunc func()

type Scheduler interface {
	Schedule(f scheduleFunc)
}

type scheduler struct {
	log      *slog.Logger
	inflight atomic.Int32
	sem      chan struct{}
	max      int
}

func (s *scheduler) Schedule(f scheduleFunc) {
	f2 := func() {
		f()
		return
	}
	// Unlimited if max <= 0
	if s.max <= 0 {
		go func() {
			s.inflight.Add(1)
			defer s.inflight.Add(-1)
			f2()
		}()
		return
	}

	// Bounded by semaphore
	go func() {
		s.sem <- struct{}{}
		s.inflight.Add(1)
		defer func() {
			<-s.sem
			s.inflight.Add(-1)
		}()

		f2()
	}()
}

// NewScheduler creates a simple scheduler that limits the number of
// concurrently running tasks to max. If max <= 0, concurrency is unlimited.
func NewScheduler(max int) Scheduler {
	var sem chan struct{}
	if max > 0 {
		sem = make(chan struct{}, max)
	}
	return &scheduler{sem: sem, max: max, log: slog.Default()}
}
