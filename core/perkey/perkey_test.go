package perkey

import (
	"context"
	"errors"
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
