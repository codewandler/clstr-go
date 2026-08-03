package actor

import (
	"context"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A backlog behind a saturated executor must cost heap, not goroutines: the
// old spawn-then-park design held one live goroutine per queued task, growing
// the process by one stack per pending task until OOMKill (DEV-726).
func TestScheduler_BacklogBoundedGoroutines(t *testing.T) {
	const (
		maxConcurrent = 8
		submitted     = 400
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	release := make(chan struct{})
	var ran atomic.Int32
	sched := NewScheduler(maxConcurrent, ctx)

	runtime.GC()
	before := runtime.NumGoroutine()

	for range submitted {
		sched.Schedule(func() {
			<-release
			ran.Add(1)
		})
	}

	waitForStableGoroutines(t)
	peak := runtime.NumGoroutine() - before
	t.Logf("cap %d, submitted %d, goroutines above baseline: %d", maxConcurrent, submitted, peak)

	close(release)
	sched.Wait()

	assert.LessOrEqual(t, peak, maxConcurrent+4,
		"queued tasks must not park goroutines: got %d above baseline for a %d-task backlog",
		peak, submitted)
	assert.Equal(t, int32(submitted), ran.Load(), "every queued task must eventually run")
}

// Cancellation drops still-queued tasks without running them (the pre-existing
// contract) and must not strand Wait().
func TestScheduler_CancelDropsQueuedTasks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	release := make(chan struct{})
	started := make(chan struct{})
	var ran atomic.Int32
	sched := NewScheduler(1, ctx)

	sched.Schedule(func() {
		close(started)
		<-release
		ran.Add(1)
	})
	<-started // the single slot is now busy; everything below queues

	for range 10 {
		sched.Schedule(func() { ran.Add(1) })
	}

	cancel()
	close(release)

	done := make(chan struct{})
	go func() { sched.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Wait() did not return after cancellation")
	}

	require.Equal(t, int32(1), ran.Load(),
		"only the already-running task may complete; queued tasks are dropped on cancellation")
}

// waitForStableGoroutines waits until the goroutine count stops changing so
// assertions measure a settled state, not a race with goroutine startup.
func waitForStableGoroutines(t *testing.T) {
	t.Helper()
	last := -1
	for range 100 {
		time.Sleep(5 * time.Millisecond)
		if n := runtime.NumGoroutine(); n == last {
			return
		} else {
			last = n
		}
	}
}
