package es_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/codewandler/clstr-go/core/es"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

// signalError sends an error to the done channel (simulates unexpected subscription failure).
func (m *mockSub) signalError(err error) { m.done <- err }

// signalCleanShutdown closes the done channel (simulates clean context cancellation).
func (m *mockSub) signalCleanShutdown() { close(m.done) }

type mockStore struct {
	sub es.Subscription
}

func (m *mockStore) Subscribe(_ context.Context, _ ...es.SubscribeOption) (es.Subscription, error) {
	return m.sub, nil
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

// TestConsumer_DoneErrorBeforeLive asserts that when the subscription signals an
// error via Done() before the consumer has gone live, Start() returns that error
// within 1 second rather than blocking forever.
func TestConsumer_DoneErrorBeforeLive(t *testing.T) {
	sub := newMockSub(100) // liveAt=100, consumer won't go live on its own
	consumer := es.NewConsumer(&mockStore{sub: sub}, &passthroughDecoder{}, &noopHandler{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	startErrCh := make(chan error, 1)
	go func() { startErrCh <- consumer.Start(ctx) }()

	// Let the consumer goroutine start up
	sendEvents(sub, 5)
	time.Sleep(20 * time.Millisecond)

	// Simulate unexpected subscription failure (NATS connection drop, etc.)
	sub.signalError(fmt.Errorf("connection reset by peer"))

	select {
	case err := <-startErrCh:
		require.Error(t, err, "Start() must return an error when subscription fails before live")
		t.Logf("Start() returned: %v", err)
	case <-time.After(1 * time.Second):
		t.Fatal("Start() did not return within 1s after Done() error signal")
	}
}

// TestConsumer_ClosedChannelExitsCleanly asserts that when the subscription channel
// is closed without a Done() signal (defensive guard), the consumer exits cleanly
// and Start() returns an error within 1 second (rather than spinning forever).
//
// This test was the primary regression check: before the fix, Start() would block
// forever because the consumer goroutine spun reading zero-value envelopes.
func TestConsumer_ClosedChannelExitsCleanly(t *testing.T) {
	sub := newMockSub(100)
	consumer := es.NewConsumer(&mockStore{sub: sub}, &passthroughDecoder{}, &noopHandler{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	startErrCh := make(chan error, 1)
	go func() { startErrCh <- consumer.Start(ctx) }()

	sendEvents(sub, 5)
	time.Sleep(20 * time.Millisecond)

	// Close channel without signaling Done() — simulates a non-compliant
	// subscription implementation or a race where ch closes before done is sent.
	close(sub.ch)

	select {
	case err := <-startErrCh:
		require.Error(t, err, "Start() must return an error when channel closes without Done signal")
		t.Logf("Start() returned: %v", err)
	case <-time.After(1 * time.Second):
		t.Fatal("BUG: Start() blocked after subscription channel was closed (infinite spin)")
	}
}

// TestConsumer_HappyPath asserts that when exactly maxSeq events arrive
// (consumer catches up and goes live), Start() returns nil.
func TestConsumer_HappyPath(t *testing.T) {
	const n = uint64(5)
	sub := newMockSub(n) // liveAt = n, consumer goes live after receiving seq n

	consumer := es.NewConsumer(&mockStore{sub: sub}, &passthroughDecoder{}, &noopHandler{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	startErrCh := make(chan error, 1)
	go func() { startErrCh <- consumer.Start(ctx) }()

	// Send exactly n events to bring the consumer live
	sendEvents(sub, n)

	select {
	case err := <-startErrCh:
		assert.NoError(t, err, "Start() should return nil when consumer goes live normally")
	case <-time.After(2 * time.Second):
		t.Fatal("Start() did not return within 2s after consumer became live")
	}
}

// TestConsumer_DoneErrorAfterLive asserts that when the subscription signals an
// error via Done() after the consumer has already gone live, the consumer goroutine
// exits cleanly (no spin, no panic) and Start() is not affected (already returned nil).
func TestConsumer_DoneErrorAfterLive(t *testing.T) {
	const n = uint64(5)
	sub := newMockSub(n)

	consumer := es.NewConsumer(&mockStore{sub: sub}, &passthroughDecoder{}, &noopHandler{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	startErrCh := make(chan error, 1)
	go func() { startErrCh <- consumer.Start(ctx) }()

	// Bring consumer live
	sendEvents(sub, n)

	select {
	case err := <-startErrCh:
		require.NoError(t, err, "Start() should return nil when consumer goes live")
	case <-time.After(2 * time.Second):
		t.Fatal("Start() did not return within 2s")
	}

	// Consumer is now live and Start() has returned.
	// Signal an error on Done() — the background goroutine should exit cleanly.
	sub.signalError(fmt.Errorf("connection reset after going live"))

	// Give the goroutine time to exit. There is no spin, so this should be instant.
	// We verify the goroutine has exited by stopping the consumer (which waits on c.done).
	stopped := make(chan struct{})
	go func() {
		consumer.Stop()
		close(stopped)
	}()

	select {
	case <-stopped:
		// Goroutine exited cleanly
	case <-time.After(2 * time.Second):
		t.Fatal("consumer goroutine did not exit within 2s after Done() error signal post-live")
	}
}
