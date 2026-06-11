// Package perkey provides a scheduler that serializes work per key
// while allowing work for different keys to execute concurrently.
//
// Typical use-case: event-sourced aggregates, where you want to process
// commands per aggregate ID sequentially, but different aggregates in parallel.
//
// Worker goroutines live only as long as their key has work: once a worker
// has no queued or claimed tasks left it removes itself from the scheduler
// and exits. A later Do for the same key starts a fresh worker. Per-key
// ordering is unaffected, because a worker only retires when nothing for its
// key remains.
package perkey

import (
	"context"
	"sync"
)

// Option configures a Scheduler.
type Option func(*config)

type config struct {
	bufferSize int
}

// WithBufferSize sets the task buffer size per worker (default: 64).
func WithBufferSize(size int) Option {
	return func(c *config) {
		if size > 0 {
			c.bufferSize = size
		}
	}
}

// Scheduler runs tasks (functions) such that for any given key K,
// tasks are executed sequentially, in submission order.
// Tasks for *different* keys can proceed in parallel.
type Scheduler[K comparable] struct {
	mu         sync.Mutex
	workers    map[K]*worker
	closed     bool
	wg         sync.WaitGroup // tracks in-flight Do operations
	bufferSize int
}

type worker struct {
	tasks chan *task
	// pending counts tasks claimed for this worker that have not finished:
	// incremented in DoContext when the worker is picked, decremented after
	// the task ran or when an enqueue is abandoned on context cancellation.
	// Guarded by Scheduler.mu. The worker retires when it drops to zero.
	pending int
}

type task struct {
	fn   func() error
	done chan error
}

// New creates a new Scheduler.
func New[K comparable](opts ...Option) *Scheduler[K] {
	cfg := &config{bufferSize: 64}
	for _, opt := range opts {
		opt(cfg)
	}
	return &Scheduler[K]{
		workers:    make(map[K]*worker),
		bufferSize: cfg.bufferSize,
	}
}

// Do schedules fn to run for the given key.
// It blocks until fn finishes and returns its error.
// All fn calls for the same key are executed sequentially.
func (s *Scheduler[K]) Do(key K, fn func() error) error {
	return s.DoContext(context.Background(), key, fn)
}

// DoContext is like Do but respects context cancellation.
// If the context is cancelled while waiting to enqueue or waiting for
// completion, it returns the context error. Note that if a task is already
// enqueued, it will still execute even if the caller's context is cancelled.
func (s *Scheduler[K]) DoContext(ctx context.Context, key K, fn func() error) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrSchedulerClosed
	}
	s.wg.Add(1)
	w := s.getOrCreateWorkerLocked(key)
	w.pending++
	s.mu.Unlock()

	t := &task{
		fn:   fn,
		done: make(chan error, 1),
	}

	// Enqueue task or respect context cancellation.
	select {
	case w.tasks <- t:
		// Task enqueued successfully.
	case <-ctx.Done():
		s.abandonTask(key, w)
		s.wg.Done()
		return ctx.Err()
	}

	// Wait for completion or context cancellation.
	select {
	case err := <-t.done:
		s.wg.Done()
		return err
	case <-ctx.Done():
		// Task is already in the queue and will execute,
		// but we don't wait for it.
		s.wg.Done()
		return ctx.Err()
	}
}

// Close stops accepting new tasks and shuts down all workers.
// It waits for in-flight Do operations to finish enqueueing before
// closing worker channels. Existing tasks in queues will still be processed.
func (s *Scheduler[K]) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.mu.Unlock()

	// Wait for all in-flight Do operations to finish enqueueing.
	// This prevents sends to closed channels.
	s.wg.Wait()

	// Now safe to close all worker channels.
	s.mu.Lock()
	for _, w := range s.workers {
		close(w.tasks)
	}
	s.workers = nil
	s.mu.Unlock()
}

// abandonTask releases the pending claim of a task that was never enqueued.
// If that leaves the worker with nothing queued or claimed, the worker is
// removed from the map and its channel closed so its goroutine exits.
func (s *Scheduler[K]) abandonTask(key K, w *worker) {
	s.mu.Lock()
	w.pending--
	if w.pending > 0 {
		s.mu.Unlock()
		return
	}
	delete(s.workers, key)
	s.mu.Unlock()
	// Safe to close: pending was zero under mu, so no other goroutine holds a
	// claim (every sender increments pending before sending), and the worker
	// is out of the map, so no new claim can appear. Close cannot race this
	// close either — it waits in wg.Wait() until this Do returns, and by then
	// the worker is no longer in the map.
	close(w.tasks)
}

func (s *Scheduler[K]) getOrCreateWorkerLocked(key K) *worker {
	w, ok := s.workers[key]
	if ok {
		return w
	}

	w = &worker{
		tasks: make(chan *task, s.bufferSize),
	}
	s.workers[key] = w
	go s.runWorker(key, w)

	return w
}

// runWorker processes tasks sequentially for a single key. It exits when its
// channel is closed (Close, or an abandoned enqueue that retired the worker)
// or when no queued or claimed tasks remain for the key, in which case it
// removes itself from the worker map so idle keys do not pin goroutines.
func (s *Scheduler[K]) runWorker(key K, w *worker) {
	for {
		t, ok := <-w.tasks
		if !ok {
			return
		}
		t.done <- t.fn()

		s.mu.Lock()
		w.pending--
		if w.pending == 0 {
			// Nothing queued or claimed for this key: retire. A concurrent
			// DoContext for the same key serializes on mu and either lands
			// its claim before this check (pending > 0, we keep running) or
			// finds the map entry gone and starts a fresh worker. delete on
			// the nil map left behind by Close is a no-op.
			delete(s.workers, key)
			s.mu.Unlock()
			return
		}
		s.mu.Unlock()
	}
}

// ----- Errors -----

// ErrSchedulerClosed is returned when Do is called on a closed scheduler.
var ErrSchedulerClosed = &SchedulerError{"scheduler is closed"}

// SchedulerError is a simple error implementation.
type SchedulerError struct {
	msg string
}

func (e *SchedulerError) Error() string { return e.msg }
