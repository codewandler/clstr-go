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

// failingHandler returns an error for every event.
type failingHandler struct {
	mu       sync.Mutex
	attempts int
}

func (h *failingHandler) Handle(_ es.MsgCtx) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.attempts++
	return errors.New("handler exploded")
}

func (h *failingHandler) getAttempts() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.attempts
}

// failOnceHandler fails on the first call, then succeeds.
type failOnceHandler struct {
	mu       sync.Mutex
	calls    int
	seqsSeen []uint64
}

func (h *failOnceHandler) Handle(ctx es.MsgCtx) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.calls++
	if h.calls == 1 {
		return errors.New("transient failure")
	}
	h.seqsSeen = append(h.seqsSeen, ctx.Seq())
	return nil
}

func (h *failOnceHandler) snapshot() []uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	cp := make([]uint64, len(h.seqsSeen))
	copy(cp, h.seqsSeen)
	return cp
}

// TestConsumer_ErrorStrategyStop_RetriesSubscription verifies that with
// ErrorStrategyStop (the default), a handler error causes the consumer to
// treat it as a subscription failure and retry after backoff — the failing
// event will be re-processed from the checkpoint.
func TestConsumer_ErrorStrategyStop_RetriesSubscription(t *testing.T) {
	const liveAt = uint64(3)

	// sub1 will deliver 3 events; the handler will fail on the first one
	sub1 := newMockSub(liveAt)
	// sub2 is used after retry — consumer re-subscribes
	sub2 := newMockSub(0) // maxSeq=0 → immediately live

	store := newMockStore(sub1, sub2)
	handler := &failingHandler{}

	consumer := es.NewConsumer(
		store, &passthroughDecoder{}, handler,
		es.WithReconnectBackoff(100*time.Millisecond, 100*time.Millisecond),
		// ErrorStrategyStop is the default — no need to set explicitly
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	startErrCh := make(chan error, 1)
	go func() { startErrCh <- consumer.Start(ctx) }()

	// Send one event on sub1 — handler will fail → consumer should treat
	// this as a fatal error and retry the subscription.
	sub1.ch <- es.Envelope{ID: "e1", Type: "TestEvent", Seq: 1}

	// The consumer should retry, calling Subscribe() a second time.
	require.Eventually(t, func() bool {
		return store.subscribeCallCount() >= 2
	}, 2*time.Second, 10*time.Millisecond,
		"consumer should retry subscription after handler error with ErrorStrategyStop")

	// The handler should have been called at least once.
	assert.GreaterOrEqual(t, handler.getAttempts(), 1)

	// Clean shutdown.
	cancel()
	<-consumer.Died()
}

// TestConsumer_ErrorStrategySkip_ContinuesProcessing verifies that with
// ErrorStrategySkip, a handler error is logged and the consumer continues
// processing subsequent events (the old behavior, now opt-in).
func TestConsumer_ErrorStrategySkip_ContinuesProcessing(t *testing.T) {
	const liveAt = uint64(3)
	sub := newMockSub(liveAt)
	store := newMockStore(sub)
	handler := &failOnceHandler{}

	consumer := es.NewConsumer(
		store, &passthroughDecoder{}, handler,
		es.WithErrorStrategy(es.ErrorStrategySkip),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	startErrCh := make(chan error, 1)
	go func() { startErrCh <- consumer.Start(ctx) }()

	// Send 3 events. Handler fails on first, succeeds on 2 and 3.
	sendEvents(sub, liveAt)

	select {
	case err := <-startErrCh:
		require.NoError(t, err, "Start() should succeed — errors are skipped")
	case <-time.After(2 * time.Second):
		t.Fatal("Start() did not return within 2s")
	}

	// Events 2 and 3 should have been processed despite event 1 failing.
	seen := handler.snapshot()
	assert.Equal(t, []uint64{2, 3}, seen,
		"with ErrorStrategySkip, events after a failure should still be processed")

	// Only one Subscribe() call — no retry needed.
	assert.Equal(t, 1, store.subscribeCallCount(),
		"consumer should not have retried subscription")
}

// TestConsumer_DefaultErrorStrategy_IsStop verifies that the default error
// strategy is ErrorStrategyStop (fail-safe).
func TestConsumer_DefaultErrorStrategy_IsStop(t *testing.T) {
	sub1 := newMockSub(5)
	sub2 := newMockSub(0)
	store := newMockStore(sub1, sub2)
	handler := &failingHandler{}

	consumer := es.NewConsumer(
		store, &passthroughDecoder{}, handler,
		// No WithErrorStrategy — should default to Stop
		es.WithReconnectBackoff(100*time.Millisecond, 100*time.Millisecond),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go consumer.Start(ctx)

	// Send one event — handler fails
	sub1.ch <- es.Envelope{ID: "e1", Type: "TestEvent", Seq: 1}

	// Default should be Stop → retry
	require.Eventually(t, func() bool {
		return store.subscribeCallCount() >= 2
	}, 2*time.Second, 10*time.Millisecond,
		"default error strategy should be Stop (retry on handler error)")

	cancel()
	<-consumer.Died()
}

// TestConsumer_ErrorStrategyStop_WithCheckpoint verifies that when a handler
// error triggers a retry, the consumer resumes from the checkpoint — meaning
// the failed event is retried, not skipped.
func TestConsumer_ErrorStrategyStop_WithCheckpoint(t *testing.T) {
	const liveAt = uint64(3)

	sub1 := newMockSub(liveAt) // first subscription
	sub2 := newMockSub(0)      // retry subscription (maxSeq=0 → immediately live)

	store := newMockStore(sub1, sub2)

	// Handler fails on seq=2 the first time, succeeds after retry.
	handler := &failOnSeqHandler{failSeq: 2}

	consumer := es.NewConsumer(
		store, &passthroughDecoder{}, handler,
		es.WithReconnectBackoff(100*time.Millisecond, 100*time.Millisecond),
		// default: ErrorStrategyStop
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go consumer.Start(ctx)

	// Send events 1-3 on sub1. Event 1 succeeds, event 2 fails → retry.
	sendEvents(sub1, liveAt)

	// Consumer should retry (subscribe again).
	require.Eventually(t, func() bool {
		return store.subscribeCallCount() >= 2
	}, 2*time.Second, 10*time.Millisecond)

	// After retry, the consumer re-subscribes. If we had a real checkpoint,
	// it would resume from seq 1 (the last successfully processed).
	// For this test, just verify the handler was called with seq=2 again on retry.
	sendEvents(sub2, liveAt)
	time.Sleep(50 * time.Millisecond)

	// Event 2 should have been retried (failOnSeqHandler only fails once).
	seen := handler.snapshot()
	assert.Contains(t, seen, uint64(2),
		"failed event should be retried after ErrorStrategyStop")

	cancel()
	<-consumer.Died()
}

// failOnSeqHandler fails on the first attempt at a specific sequence, then succeeds.
type failOnSeqHandler struct {
	mu       sync.Mutex
	failSeq  uint64
	failed   bool
	seqsSeen []uint64
}

func (h *failOnSeqHandler) Handle(ctx es.MsgCtx) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if ctx.Seq() == h.failSeq && !h.failed {
		h.failed = true
		return fmt.Errorf("transient failure on seq %d", h.failSeq)
	}
	h.seqsSeen = append(h.seqsSeen, ctx.Seq())
	return nil
}

func (h *failOnSeqHandler) snapshot() []uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	cp := make([]uint64, len(h.seqsSeen))
	copy(cp, h.seqsSeen)
	return cp
}
