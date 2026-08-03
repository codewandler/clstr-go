package actor

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
)

// scheduleFunc is the signature for scheduled background tasks.
type scheduleFunc func()

// Scheduler manages execution of background tasks with optional concurrency limits.
type Scheduler interface {
	// Schedule queues a function for asynchronous execution.
	Schedule(f scheduleFunc)
	// Wait blocks until all in-flight tasks complete.
	Wait()
}

type scheduler struct {
	ctx      context.Context
	log      *slog.Logger
	inflight atomic.Int32
	max      int

	// mu guards queue and workers. Tasks wait in queue as plain function
	// values; at most max worker goroutines drain it. This bounds goroutines
	// by max regardless of backlog size — a queued task costs heap, never a
	// goroutine stack. The previous design (spawn a goroutine per Schedule,
	// park it on a semaphore) grew the process by one stack per pending task,
	// which OOMKilled long-running low-memory deployments under a slow drain
	// (DEV-726).
	mu      sync.Mutex
	queue   []scheduleFunc
	workers int

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

	// Bounded: enqueue the task and make sure a worker exists to drain it.
	s.mu.Lock()
	s.queue = append(s.queue, f)
	depth := len(s.queue)
	if s.workers < s.max {
		s.workers++
		go s.worker()
	}
	s.mu.Unlock()

	s.metrics.SchedulerQueueDepth(s.actorID, depth)
}

// worker drains the task queue and exits as soon as the queue is empty; a
// later Schedule starts a fresh worker. The worker count stays bounded by max
// under mu: a concurrent Schedule either lands its task before the
// empty-check (this worker keeps running) or after the decrement (Schedule
// sees a free slot and spawns a new worker).
func (s *scheduler) worker() {
	for {
		s.mu.Lock()
		if len(s.queue) == 0 {
			s.workers--
			s.mu.Unlock()
			return
		}
		f := s.queue[0]
		s.queue[0] = nil // release the task for GC
		s.queue = s.queue[1:]
		depth := len(s.queue)
		if depth == 0 {
			s.queue = nil // release the backing array
		}
		s.mu.Unlock()

		s.metrics.SchedulerQueueDepth(s.actorID, depth)

		// On shutdown, drop queued tasks without running them (same contract
		// as before: a task still waiting for a slot at cancellation never
		// runs), but keep draining so Wait() unblocks.
		if s.ctx.Err() != nil {
			s.wg.Done()
			continue
		}

		count := s.inflight.Add(1)
		s.metrics.SchedulerInflight(s.actorID, int(count))
		s.runTask(f)
		count = s.inflight.Add(-1)
		s.metrics.SchedulerInflight(s.actorID, int(count))
		s.wg.Done()
	}
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
	if metrics == nil {
		metrics = NopActorMetrics()
	}
	return &scheduler{
		ctx:     ctx,
		max:     max,
		log:     slog.Default(),
		actorID: actorID,
		metrics: metrics,
	}
}
