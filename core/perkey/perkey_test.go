package perkey

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestScheduler_SequentialPerKey(t *testing.T) {
	s := New[string]()
	defer s.Close()

	var seq []int
	var mu sync.Mutex

	// Schedule 3 tasks for the same key.
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = s.Do("key1", func() error {
				mu.Lock()
				seq = append(seq, i)
				mu.Unlock()
				time.Sleep(10 * time.Millisecond)
				return nil
			})
		}()
		// Small delay to ensure ordering.
		time.Sleep(2 * time.Millisecond)
	}
	wg.Wait()

	// Should execute in order 0, 1, 2.
	if len(seq) != 3 {
		t.Fatalf("expected 3 executions, got %d", len(seq))
	}
	for i, v := range seq {
		if v != i {
			t.Errorf("expected seq[%d]=%d, got %d", i, i, v)
		}
	}
}

func TestScheduler_ParallelAcrossKeys(t *testing.T) {
	s := New[string]()
	defer s.Close()

	var running atomic.Int32
	var maxRunning atomic.Int32

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		key := string(rune('a' + i))
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = s.Do(key, func() error {
				cur := running.Add(1)
				// Track max concurrent.
				for {
					max := maxRunning.Load()
					if cur <= max || maxRunning.CompareAndSwap(max, cur) {
						break
					}
				}
				time.Sleep(50 * time.Millisecond)
				running.Add(-1)
				return nil
			})
		}()
	}
	wg.Wait()

	// Should have run at least 2 concurrently (different keys).
	if maxRunning.Load() < 2 {
		t.Errorf("expected concurrent execution across keys, max running was %d", maxRunning.Load())
	}
}

func TestScheduler_ErrorPropagation(t *testing.T) {
	s := New[string]()
	defer s.Close()

	expectedErr := errors.New("task error")
	err := s.Do("key", func() error {
		return expectedErr
	})
	if err != expectedErr {
		t.Errorf("expected %v, got %v", expectedErr, err)
	}
}

func TestScheduler_DoContext_Cancelled(t *testing.T) {
	s := New[string]()
	defer s.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately.

	err := s.DoContext(ctx, "key", func() error {
		t.Error("task should not execute")
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

func TestScheduler_DoContext_Timeout(t *testing.T) {
	s := New[string]()
	defer s.Close()

	// First task blocks for a while.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = s.Do("key", func() error {
			time.Sleep(200 * time.Millisecond)
			return nil
		})
	}()

	// Give first task time to start.
	time.Sleep(10 * time.Millisecond)

	// Second task with short timeout should time out waiting.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	err := s.DoContext(ctx, "key", func() error {
		return nil
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected context.DeadlineExceeded, got %v", err)
	}

	wg.Wait()
}

func TestScheduler_Close_NoNewTasks(t *testing.T) {
	s := New[string]()
	s.Close()

	err := s.Do("key", func() error {
		return nil
	})
	if err != ErrSchedulerClosed {
		t.Errorf("expected ErrSchedulerClosed, got %v", err)
	}
}

func TestScheduler_Close_DrainsExisting(t *testing.T) {
	s := New[string](WithBufferSize(10))

	var executed atomic.Int32

	// Enqueue several tasks.
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = s.Do("key", func() error {
				time.Sleep(10 * time.Millisecond)
				executed.Add(1)
				return nil
			})
		}()
	}

	// Give tasks time to enqueue.
	time.Sleep(20 * time.Millisecond)

	// Close should wait for in-flight operations and drain queued tasks.
	s.Close()
	wg.Wait()

	if executed.Load() != 5 {
		t.Errorf("expected 5 tasks executed, got %d", executed.Load())
	}
}

func TestScheduler_Close_NoPanic(t *testing.T) {
	// This test verifies the race condition fix.
	// Run with -race to detect races.
	s := New[string]()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = s.Do("key", func() error {
				return nil
			})
		}()
	}

	// Close while tasks are being submitted.
	go func() {
		time.Sleep(time.Millisecond)
		s.Close()
	}()

	wg.Wait()
}

func TestScheduler_Close_Idempotent(t *testing.T) {
	s := New[string]()
	s.Close()
	s.Close() // Should not panic.
}

func TestScheduler_WithBufferSize(t *testing.T) {
	s := New[string](WithBufferSize(2))
	defer s.Close()

	blocked := make(chan struct{})
	release := make(chan struct{})

	// Start a blocking task.
	go func() {
		_ = s.Do("key", func() error {
			close(blocked)
			<-release
			return nil
		})
	}()

	<-blocked // Wait for task to start.

	// With buffer size 2, we can enqueue 2 more tasks without blocking.
	for i := 0; i < 2; i++ {
		go func() {
			_ = s.Do("key", func() error { return nil })
		}()
	}

	// Give time for tasks to enqueue.
	time.Sleep(10 * time.Millisecond)

	close(release)
}

func TestScheduler_WithBufferSize_Invalid(t *testing.T) {
	s := New[string](WithBufferSize(0))   // Should use default.
	s2 := New[string](WithBufferSize(-1)) // Should use default.
	defer s.Close()
	defer s2.Close()

	// Should work normally with default buffer.
	err := s.Do("key", func() error { return nil })
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestSchedulerError(t *testing.T) {
	err := &SchedulerError{msg: "test error"}
	if err.Error() != "test error" {
		t.Errorf("expected 'test error', got %q", err.Error())
	}
}

func TestScheduler_ManyKeys(t *testing.T) {
	s := New[int]()
	defer s.Close()

	var wg sync.WaitGroup
	var total atomic.Int32

	for i := 0; i < 100; i++ {
		key := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = s.Do(key, func() error {
				total.Add(1)
				return nil
			})
		}()
	}

	wg.Wait()

	if total.Load() != 100 {
		t.Errorf("expected 100 executions, got %d", total.Load())
	}
}

// workerCount reports the number of live per-key workers (test helper).
func (s *Scheduler[K]) workerCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.workers)
}

// eventually polls cond until it returns true or the deadline expires.
func eventually(t *testing.T, d time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal(msg)
}

func TestScheduler_RetiresIdleWorkers(t *testing.T) {
	s := New[int]()
	defer s.Close()

	for i := 0; i < 1000; i++ {
		if err := s.Do(i, func() error { return nil }); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	// Do returns when the task's result is delivered; the worker's
	// self-retirement happens just after, so poll briefly.
	eventually(t, 2*time.Second, func() bool { return s.workerCount() == 0 },
		"idle workers were not retired")
}

func TestScheduler_NoGoroutineLeakManyKeys(t *testing.T) {
	s := New[int]()
	defer s.Close()

	before := runtime.NumGoroutine()

	for i := 0; i < 500; i++ {
		_ = s.Do(i, func() error { return nil })
	}

	eventually(t, 2*time.Second, func() bool {
		runtime.GC()
		return runtime.NumGoroutine() <= before+5
	}, "goroutines did not return to baseline after 500 distinct keys")
}

func TestScheduler_KeyReusableAfterRetirement(t *testing.T) {
	s := New[string]()
	defer s.Close()

	var runs atomic.Int32
	if err := s.Do("key", func() error { runs.Add(1); return nil }); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	eventually(t, 2*time.Second, func() bool { return s.workerCount() == 0 },
		"worker was not retired after first task")

	// A fresh worker must be created transparently for the same key.
	if err := s.Do("key", func() error { runs.Add(1); return nil }); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if runs.Load() != 2 {
		t.Fatalf("expected 2 executions, got %d", runs.Load())
	}
}

func TestScheduler_BusyWorkerSurvivesRetirementCheck(t *testing.T) {
	s := New[string]()
	defer s.Close()

	var seq []int
	var mu sync.Mutex

	release := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_ = s.Do("key", func() error {
			mu.Lock()
			seq = append(seq, 1)
			mu.Unlock()
			<-release
			return nil
		})
	}()
	// Queue a second task behind the first so the worker is never idle in
	// between; it must not retire with work still claimed.
	time.Sleep(10 * time.Millisecond)
	go func() {
		defer wg.Done()
		_ = s.Do("key", func() error {
			mu.Lock()
			seq = append(seq, 2)
			mu.Unlock()
			return nil
		})
	}()
	time.Sleep(10 * time.Millisecond)
	close(release)
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(seq) != 2 || seq[0] != 1 || seq[1] != 2 {
		t.Fatalf("expected ordered execution [1 2], got %v", seq)
	}
}

func TestScheduler_AbandonedEnqueueReleasesClaim(t *testing.T) {
	s := New[string](WithBufferSize(1))
	defer s.Close()

	blocked := make(chan struct{})
	release := make(chan struct{})

	// Occupy the worker.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = s.Do("key", func() error {
			close(blocked)
			<-release
			return nil
		})
	}()
	<-blocked

	// Fill the buffer (size 1).
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = s.Do("key", func() error { return nil })
	}()
	time.Sleep(10 * time.Millisecond)

	// This enqueue blocks on the full buffer; cancelling must abandon the
	// claim without disturbing the queued work.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := s.DoContext(ctx, "key", func() error {
		t.Error("abandoned task must not execute")
		return nil
	}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}

	close(release)
	wg.Wait()

	eventually(t, 2*time.Second, func() bool { return s.workerCount() == 0 },
		"worker was not retired after abandoned enqueue")
}

func TestScheduler_ConcurrentCancellationStress(t *testing.T) {
	s := New[int](WithBufferSize(1))
	defer s.Close()

	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		key := i % 10
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithCancel(context.Background())
			// Cancel concurrently with the enqueue/wait to exercise both
			// select branches, including abandonTask's worker retirement.
			go cancel()
			_ = s.DoContext(ctx, key, func() error {
				time.Sleep(time.Millisecond)
				return nil
			})
		}()
	}
	wg.Wait()

	eventually(t, 5*time.Second, func() bool { return s.workerCount() == 0 },
		"workers were not retired after concurrent cancellations")
}
