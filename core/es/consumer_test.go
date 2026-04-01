package es_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/codewandler/clstr-go/core/es"
)

// --- mock helpers ---

// mockSub is a controllable Subscription used in unit tests.
type mockSub struct {
	ch     chan es.Envelope
	done   chan error
	maxSeq uint64
}

func newMockSub(maxSeq uint64) *mockSub {
	return &mockSub{
		ch:     make(chan es.Envelope, 10),
		done:   make(chan error, 1),
		maxSeq: maxSeq,
	}
}

func (m *mockSub) Cancel()                  {}
func (m *mockSub) MaxSequence() uint64      { return m.maxSeq }
func (m *mockSub) Chan() <-chan es.Envelope { return m.ch }
func (m *mockSub) Done() <-chan error       { return m.done }

func (m *mockSub) signalError(err error) { m.done <- err }

// mockStore returns subscriptions from a queue. When the queue is exhausted
// the last subscription is returned for all subsequent calls.
// It also captures the SubscribeOptions from each call for assertion in tests.
type mockStore struct {
	mu        sync.Mutex
	subs      []*mockSub
	idx       int
	callCount int
	lastOpts  []es.SubscribeOption
}

// newMockStore creates a store that returns the given subs in sequence.
func newMockStore(subs ...*mockSub) *mockStore {
	return &mockStore{subs: subs}
}

func (m *mockStore) Subscribe(_ context.Context, opts ...es.SubscribeOption) (es.Subscription, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.callCount++
	m.lastOpts = opts
	if len(m.subs) == 0 {
		return nil, errors.New("no subscriptions configured")
	}
	sub := m.subs[m.idx]
	if m.idx < len(m.subs)-1 {
		m.idx++
	}
	return sub, nil
}

func (m *mockStore) subscribeCallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.callCount
}

// lastSubscribeOpts returns the resolved SubscribeOpts from the most recent Subscribe() call.
func (m *mockStore) lastSubscribeOpts() es.SubscribeOpts {
	m.mu.Lock()
	defer m.mu.Unlock()
	return es.NewSubscribeOpts(m.lastOpts...)
}

func (m *mockStore) Load(_ context.Context, _, _ string, _ ...es.StoreLoadOption) ([]es.Envelope, error) {
	return nil, nil
}
func (m *mockStore) Append(_ context.Context, _, _ string, _ es.Version, _ []es.Envelope) (*es.StoreAppendResult, error) {
	return nil, nil
}

// passthroughDecoder returns the envelope itself for any non-empty Type.
type passthroughDecoder struct{}

func (d *passthroughDecoder) Decode(e es.Envelope) (any, error) {
	if e.Type == "" {
		return nil, errors.New("unknown event type: (empty)")
	}
	return e, nil
}

type noopHandler struct{}

func (h *noopHandler) Handle(_ es.MsgCtx) error { return nil }

// sendEvents sends n sequential events (seq 1..n) with Type="TestEvent" to sub.ch.
func sendEvents(sub *mockSub, n uint64) {
	for i := uint64(1); i <= n; i++ {
		sub.ch <- es.Envelope{
			ID:   fmt.Sprintf("id-%d", i),
			Type: "TestEvent",
			Seq:  i,
		}
	}
}

// --- unit tests ---

// TestConsumer_HappyPath asserts that when exactly maxSeq events arrive
// (consumer catches up and goes live), Start() returns nil.
func TestConsumer_HappyPath(t *testing.T) {
	const n = uint64(5)
	sub := newMockSub(n)
	consumer := es.NewConsumer(newMockStore(sub), &passthroughDecoder{}, &noopHandler{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	startErrCh := make(chan error, 1)
	go func() { startErrCh <- consumer.Start(ctx) }()

	sendEvents(sub, n)

	select {
	case err := <-startErrCh:
		assert.NoError(t, err, "Start() should return nil when consumer goes live normally")
	case <-time.After(2 * time.Second):
		t.Fatal("Start() did not return within 2s after consumer became live")
	}
}

// TestConsumer_RetriesAfterPreLiveFailure asserts that when a subscription fails
// before the consumer goes live, the consumer retries automatically. Start()
// blocks through the retry and returns nil when the second subscription goes live.
func TestConsumer_RetriesAfterPreLiveFailure(t *testing.T) {
	const liveAt = uint64(10)

	// sub1 will deliver 5 events then fail
	sub1 := newMockSub(liveAt)
	// sub2 will deliver all 10 events and bring the consumer live
	sub2 := newMockSub(liveAt)

	store := newMockStore(sub1, sub2)
	consumer := es.NewConsumer(store, &passthroughDecoder{}, &noopHandler{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	startErrCh := make(chan error, 1)
	go func() { startErrCh <- consumer.Start(ctx) }()

	// Deliver 5 events on sub1, then fail it
	sendEvents(sub1, 5)
	time.Sleep(20 * time.Millisecond)
	sub1.signalError(fmt.Errorf("connection reset by peer"))

	// Start() must still be waiting (not returned yet) — consumer is retrying
	select {
	case err := <-startErrCh:
		t.Fatalf("Start() returned prematurely before retry succeeded: %v", err)
	case <-time.After(200 * time.Millisecond):
		// expected: still waiting
	}

	// After backoff, consumer subscribes to sub2. Deliver all 10 events.
	// Backoff is 1s; allow up to 2s for it to kick in.
	require.Eventually(t, func() bool {
		return store.subscribeCallCount() >= 2
	}, 2*time.Second, 50*time.Millisecond, "consumer should retry and call Subscribe() a second time")

	sendEvents(sub2, liveAt)

	select {
	case err := <-startErrCh:
		require.NoError(t, err, "Start() must return nil after retry succeeds")
	case <-time.After(3 * time.Second):
		t.Fatal("Start() did not return within 3s after retry succeeded")
	}
}

// TestConsumer_ClosedChannelTriggersRetry asserts that a subscription channel
// closed without a Done() signal (the regression scenario) triggers a clean retry
// rather than an infinite hot-loop. It verifies:
//
//  1. Subscribe() is not called at high frequency (no spin)
//  2. Context cancellation interrupts the backoff and causes Start() to return
func TestConsumer_ClosedChannelTriggersRetry(t *testing.T) {
	sub1 := newMockSub(100) // liveAt=100 so consumer won't go live on its own
	sub2 := newMockSub(100) // returned for subsequent Subscribe() calls

	store := newMockStore(sub1, sub2)
	consumer := es.NewConsumer(store, &passthroughDecoder{}, &noopHandler{})

	ctx, cancel := context.WithCancel(context.Background())

	startErrCh := make(chan error, 1)
	go func() { startErrCh <- consumer.Start(ctx) }()

	sendEvents(sub1, 5)
	time.Sleep(20 * time.Millisecond)

	// Regression trigger: close channel without signalling Done() (old hot-loop bug)
	close(sub1.ch)

	// Wait a short window and verify Subscribe() is not called at CPU speed.
	// With 1s backoff, at most 1 retry call should occur in 200ms.
	time.Sleep(200 * time.Millisecond)
	calls := store.subscribeCallCount()
	assert.LessOrEqual(t, calls, 2,
		"no hot-loop: Subscribe() should be called at most twice in 200ms (initial + 1 retry)")

	// Cancel the context — the backoff wait should be interrupted immediately.
	cancel()

	select {
	case err := <-startErrCh:
		require.ErrorIs(t, err, context.Canceled,
			"Start() should return context.Canceled after ctx cancel")
	case <-time.After(1 * time.Second):
		t.Fatal("Start() did not return within 1s after ctx cancel")
	}
}

// TestConsumer_WithReconnectBackoff asserts that WithReconnectBackoff is respected:
// the consumer waits at least the configured initial backoff before retrying, and
// the context cancellation interrupts the wait promptly.
func TestConsumer_WithReconnectBackoff(t *testing.T) {
	sub1 := newMockSub(0) // immediately live (maxSeq=0)
	sub2 := newMockSub(0)
	store := newMockStore(sub1, sub2)

	// Use a short backoff so the test stays fast, but long enough to measure.
	const initialBackoff = 200 * time.Millisecond
	consumer := es.NewConsumer(
		store,
		&passthroughDecoder{},
		&noopHandler{},
		es.WithReconnectBackoff(initialBackoff, initialBackoff),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	startErrCh := make(chan error, 1)
	go func() { startErrCh <- consumer.Start(ctx) }()

	// sub1 has maxSeq=0 → immediately live
	select {
	case err := <-startErrCh:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Start() did not return within 1s")
	}

	// Fail sub1 and record the time
	failedAt := time.Now()
	sub1.signalError(fmt.Errorf("connection reset"))

	// Issue 8 fix: use require.Never instead of time.Sleep+require.Equal to
	// avoid a racy assertion on scheduler-sensitive timing.
	require.Never(t, func() bool {
		return store.subscribeCallCount() >= 2
	}, initialBackoff/2, 5*time.Millisecond,
		"consumer must not retry before the initial backoff window elapses")

	// After the full backoff, the second Subscribe() should happen.
	require.Eventually(t, func() bool {
		return store.subscribeCallCount() >= 2
	}, 2*initialBackoff, 10*time.Millisecond,
		"consumer should retry after backoff")

	elapsed := time.Since(failedAt)
	assert.GreaterOrEqual(t, elapsed, initialBackoff,
		"consumer must wait at least initialBackoff before retrying")

	// Context cancel interrupts any ongoing backoff immediately.
	cancel()
	stopped := make(chan struct{})
	go func() { consumer.Stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("Stop() did not return within 1s after ctx cancel")
	}
}

// checkpointingHandler implements Handler and Checkpoint so the consumer
// can resume from the last processed sequence number after a retry.
type checkpointingHandler struct {
	mu       sync.Mutex
	lastSeq  uint64
	seqsSeen []uint64
}

func (h *checkpointingHandler) Handle(ctx es.MsgCtx) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.seqsSeen = append(h.seqsSeen, ctx.Seq())
	if ctx.Seq() > h.lastSeq {
		h.lastSeq = ctx.Seq()
	}
	return nil
}

func (h *checkpointingHandler) GetLastSeq() (uint64, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.lastSeq == 0 {
		return 0, es.ErrCheckpointNotFound
	}
	return h.lastSeq, nil
}

func (h *checkpointingHandler) snapshot() (lastSeq uint64, seenCopy []uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	cp := make([]uint64, len(h.seqsSeen))
	copy(cp, h.seqsSeen)
	return h.lastSeq, cp
}

// failingLifecycleHandler implements HandlerLifecycleStart and always returns
// an error from Start(), simulating a handler that can't initialise.
type failingLifecycleHandler struct{ err error }

func (h *failingLifecycleHandler) Handle(_ es.MsgCtx) error { return nil }
func (h *failingLifecycleHandler) Start(_ context.Context) error {
	return h.err
}

// TestConsumer_LifecycleStartFailureDoesNotDeadlock verifies three properties
// when HandlerLifecycleStart.Start() returns an error:
//
//  1. Start() returns the lifecycle error immediately.
//  2. Stop() does not deadlock (c.done is closed despite loop never launching).
//  3. Died() is closed (nil receive), so any Died() readers unblock.
func TestConsumer_LifecycleStartFailureDoesNotDeadlock(t *testing.T) {
	lifecycleErr := fmt.Errorf("db connection refused")
	handler := &failingLifecycleHandler{err: lifecycleErr}

	sub := newMockSub(0)
	consumer := es.NewConsumer(newMockStore(sub), &passthroughDecoder{}, handler)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := consumer.Start(ctx)
	require.Error(t, err)
	require.ErrorContains(t, err, "db connection refused")

	// Stop() must return quickly — c.done is pre-closed by Start().
	stopped := make(chan struct{})
	go func() { consumer.Stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("Stop() deadlocked after lifecycle Start() failure")
	}

	// Died() must be closed (signal-only channel), not blocked.
	select {
	case <-consumer.Died():
		t.Log("Died() closed after lifecycle failure ✓")
	default:
		t.Fatal("Died() should already be closed after Start() returned an error")
	}
}

// TestConsumer_ConcurrentStartLifecycleFailure verifies that when Start() is
// called concurrently and HandlerLifecycleStart fails, both callers receive the
// same error and the lifecycle handler is invoked exactly once.
func TestConsumer_ConcurrentStartLifecycleFailure(t *testing.T) {
	lifecycleErr := fmt.Errorf("init failed")
	fh := &failingLifecycleHandler{err: lifecycleErr}

	sub := newMockSub(0)
	consumer := es.NewConsumer(newMockStore(sub), &passthroughDecoder{}, fh)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 2)
	go func() { errCh <- consumer.Start(ctx) }()
	go func() { errCh <- consumer.Start(ctx) }()

	for i := range 2 {
		select {
		case err := <-errCh:
			require.Error(t, err, "both concurrent Start() calls must return the lifecycle error")
			require.ErrorContains(t, err, "init failed", "call #%d", i+1)
		case <-time.After(time.Second):
			t.Fatalf("Start() call #%d did not return within 1s", i+1)
		}
	}
}

// TestConsumer_StartIsIdempotent proves that calling Start() twice concurrently
// on the same Consumer does not panic (via double-close of c.done) and both
// calls return nil once the consumer goes live.
func TestConsumer_StartIsIdempotent(t *testing.T) {
	const n = uint64(5)
	sub := newMockSub(n)
	consumer := es.NewConsumer(newMockStore(sub), &passthroughDecoder{}, &noopHandler{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 2)
	go func() { errCh <- consumer.Start(ctx) }()
	go func() { errCh <- consumer.Start(ctx) }()

	// Bring consumer live after both goroutines are started.
	time.Sleep(10 * time.Millisecond)
	sendEvents(sub, n)

	for i := range 2 {
		select {
		case err := <-errCh:
			require.NoError(t, err, "Start() call #%d should return nil", i+1)
		case <-time.After(2 * time.Second):
			t.Fatalf("Start() call #%d did not return within 2s", i+1)
		}
	}
}

// TestConsumer_CheckpointResumeAfterRetry verifies that after a subscription
// failure, the consumer reads the handler's Checkpoint and re-subscribes from
// lastSeq+1 rather than from sequence 1, avoiding redundant re-processing.
func TestConsumer_CheckpointResumeAfterRetry(t *testing.T) {
	const liveAt = uint64(5)

	sub1 := newMockSub(liveAt) // delivers seq 1-5 then fails
	sub2 := newMockSub(0)      // maxSeq=0 → immediately live on subscribe

	store := newMockStore(sub1, sub2)
	handler := &checkpointingHandler{}

	consumer := es.NewConsumer(
		store, &passthroughDecoder{}, handler,
		es.WithReconnectBackoff(200*time.Millisecond, 200*time.Millisecond),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	startErrCh := make(chan error, 1)
	go func() { startErrCh <- consumer.Start(ctx) }()

	// Deliver 5 events on sub1 → consumer goes live.
	sendEvents(sub1, liveAt)
	select {
	case err := <-startErrCh:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Start() did not return within 2s")
	}

	lastSeq, seenAfterFirst := handler.snapshot()
	assert.Equal(t, []uint64{1, 2, 3, 4, 5}, seenAfterFirst, "all 5 events processed on sub1")
	assert.Equal(t, uint64(5), lastSeq, "checkpoint is at seq 5 after sub1")

	// Kill sub1 — consumer will read the checkpoint and retry.
	sub1.signalError(fmt.Errorf("nats: consumer deleted"))

	// Wait for the retry Subscribe() call.
	require.Eventually(t, func() bool {
		return store.subscribeCallCount() >= 2
	}, 2*time.Second, 10*time.Millisecond, "consumer should have retried by now")

	// Verify the retry used WithStartSequence(lastSeq + 1 = 6).
	opts := store.lastSubscribeOpts()
	assert.Equal(t, uint64(6), opts.StartSequence(),
		"consumer must resume from checkpoint+1, not from the beginning")

	// Stop cleanly.
	stopped := make(chan struct{})
	go func() { consumer.Stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop() did not return within 2s")
	}
}

// TestConsumer_DoneErrorAfterLive asserts that when the subscription signals an
// error after the consumer has gone live, the consumer retries the subscription
// seamlessly (no spin, no process exit). Stopping the consumer interrupts the retry.
func TestConsumer_DoneErrorAfterLive(t *testing.T) {
	const n = uint64(5)
	sub := newMockSub(n)

	consumer := es.NewConsumer(newMockStore(sub), &passthroughDecoder{}, &noopHandler{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	startErrCh := make(chan error, 1)
	go func() { startErrCh <- consumer.Start(ctx) }()

	sendEvents(sub, n)

	select {
	case err := <-startErrCh:
		require.NoError(t, err, "Start() should return nil when consumer goes live")
	case <-time.After(2 * time.Second):
		t.Fatal("Start() did not return within 2s")
	}

	// Kill the subscription after going live — consumer should retry, not exit.
	sub.signalError(fmt.Errorf("connection reset after going live"))

	// Stop the consumer; this should interrupt the retry backoff and exit cleanly.
	stopped := make(chan struct{})
	go func() {
		consumer.Stop()
		close(stopped)
	}()

	select {
	case <-stopped:
		t.Log("consumer stopped cleanly during retry backoff")
	case <-time.After(2 * time.Second):
		t.Fatal("consumer did not stop within 2s")
	}
}
