package actor

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
)

type scheduleFunc func()

type Scheduler interface {
	Schedule(f scheduleFunc)
	// Wait blocks until all in-flight tasks complete or context is cancelled.
	Wait()
}

type scheduler struct {
	ctx      context.Context
	log      *slog.Logger
	inflight atomic.Int32
	sem      chan struct{}
	max      int

	wg sync.WaitGroup
}

func (s *scheduler) Schedule(f scheduleFunc) {
	// Don't schedule if context is already cancelled
	select {
	case <-s.ctx.Done():
		return
	default:
	}

	s.wg.Add(1)

	// Unlimited if max <= 0
	if s.max <= 0 {
		go func() {
			defer s.wg.Done()
			s.inflight.Add(1)
			defer s.inflight.Add(-1)
			f()
		}()
		return
	}

	// Bounded by semaphore
	go func() {
		defer s.wg.Done()

		// Wait for semaphore or context cancellation
		select {
		case <-s.ctx.Done():
			return
		case s.sem <- struct{}{}:
		}

		s.inflight.Add(1)
		defer func() {
			<-s.sem
			s.inflight.Add(-1)
		}()

		f()
	}()
}

// Wait blocks until all in-flight tasks complete.
func (s *scheduler) Wait() {
	s.wg.Wait()
}

// NewScheduler creates a simple scheduler that limits the number of
// concurrently running tasks to max. If max <= 0, concurrency is unlimited.
// The scheduler respects context cancellation for graceful shutdown.
func NewScheduler(max int, ctx context.Context) Scheduler {
	var sem chan struct{}
	if max > 0 {
		sem = make(chan struct{}, max)
	}
	return &scheduler{ctx: ctx, sem: sem, max: max, log: slog.Default()}
}
