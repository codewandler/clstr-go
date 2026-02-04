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

	// metrics support
	actorID string
	metrics ActorMetrics
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
			count := s.inflight.Add(1)
			s.metrics.SchedulerInflight(s.actorID, int(count))
			defer func() {
				count := s.inflight.Add(-1)
				s.metrics.SchedulerInflight(s.actorID, int(count))
			}()
			s.runTask(f)
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

		count := s.inflight.Add(1)
		s.metrics.SchedulerInflight(s.actorID, int(count))
		defer func() {
			<-s.sem
			count := s.inflight.Add(-1)
			s.metrics.SchedulerInflight(s.actorID, int(count))
		}()

		s.runTask(f)
	}()
}

func (s *scheduler) runTask(f scheduleFunc) {
	defer s.metrics.SchedulerTaskDuration().ObserveDuration()

	defer func() {
		if r := recover(); r != nil {
			s.metrics.SchedulerTaskCompleted(false)
			// log the panic but don't re-panic
			s.log.Error("scheduled task panicked", slog.Any("recovered", r))
			return
		}
	}()

	f()
	s.metrics.SchedulerTaskCompleted(true)
}

// Wait blocks until all in-flight tasks complete.
func (s *scheduler) Wait() {
	s.wg.Wait()
}

// NewScheduler creates a simple scheduler that limits the number of
// concurrently running tasks to max. If max <= 0, concurrency is unlimited.
// The scheduler respects context cancellation for graceful shutdown.
func NewScheduler(max int, ctx context.Context) Scheduler {
	return NewSchedulerWithMetrics(max, ctx, "", NopActorMetrics())
}

// NewSchedulerWithMetrics creates a scheduler with metrics support.
func NewSchedulerWithMetrics(max int, ctx context.Context, actorID string, metrics ActorMetrics) Scheduler {
	var sem chan struct{}
	if max > 0 {
		sem = make(chan struct{}, max)
	}
	if metrics == nil {
		metrics = NopActorMetrics()
	}
	return &scheduler{
		ctx:     ctx,
		sem:     sem,
		max:     max,
		log:     slog.Default(),
		actorID: actorID,
		metrics: metrics,
	}
}
