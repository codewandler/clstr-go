// Package perkey provides a scheduler that serializes work per key
// while allowing work for different keys to execute concurrently.
//
// Typical use-case: event-sourced aggregates, where you want to process
// commands per aggregate ID sequentially, but different aggregates in parallel.
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

func (s *Scheduler[K]) getOrCreateWorkerLocked(key K) *worker {
	w, ok := s.workers[key]
	if ok {
		return w
	}

	w = &worker{
		tasks: make(chan *task, s.bufferSize),
	}
	s.workers[key] = w
	go runWorker(w)

	return w
}

// runWorker processes tasks sequentially for a single key.
func runWorker(w *worker) {
	for t := range w.tasks {
		t.done <- t.fn()
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
