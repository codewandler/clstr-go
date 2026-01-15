// Package perkey provides a scheduler that serializes work per key
// while allowing work for different keys to execute concurrently.
//
// Typical use-case: event-sourced aggregates, where you want to process
// commands per aggregate ID sequentially, but different aggregates in parallel.
package perkey

import (
	"sync"
)

// Scheduler runs tasks (functions) such that for any given key K,
// tasks are executed sequentially, in submission order.
// Tasks for *different* keys can proceed in parallel.
type Scheduler[K comparable] struct {
	mu      sync.Mutex
	workers map[K]*worker[K]
	closed  bool
}

type worker[K comparable] struct {
	key   K
	tasks chan *task
	once  sync.Once
}

type task struct {
	fn   func() error
	done chan error
}

// New creates a new Scheduler.
func New[K comparable]() *Scheduler[K] {
	return &Scheduler[K]{
		workers: make(map[K]*worker[K]),
	}
}

// Do schedules fn to run for the given key.
// It blocks until fn finishes and returns its error.
// All fn calls for the same key are executed sequentially.
func (s *Scheduler[K]) Do(key K, fn func() error) error {
	t := &task{
		fn:   fn,
		done: make(chan error, 1),
	}

	w, err := s.getOrCreateWorker(key)
	if err != nil {
		return err
	}

	// Enqueue task for this key.
	w.tasks <- t

	// Wait for completion.
	return <-t.done
}

// Close stops accepting new tasks and shuts down all workers.
// Existing tasks already in the queues will still be processed.
func (s *Scheduler[K]) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true

	// Close all worker channels to let them exit.
	for _, w := range s.workers {
		close(w.tasks)
	}
	s.mu.Unlock()
}

func (s *Scheduler[K]) getOrCreateWorker(key K) (*worker[K], error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, ErrSchedulerClosed
	}

	w, ok := s.workers[key]
	if ok {
		return w, nil
	}

	w = &worker[K]{
		key:   key,
		tasks: make(chan *task, 64), // small buffer; adjust if needed
	}
	s.workers[key] = w
	go w.run()

	return w, nil
}

// run processes tasks sequentially for a single key.
func (w *worker[K]) run() {
	for t := range w.tasks {
		// Execute the task.
		err := t.fn()
		// Signal completion.
		t.done <- err
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
