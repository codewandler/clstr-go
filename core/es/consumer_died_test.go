package es_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/codewandler/clstr-go/core/es"
)

// TestConsumer_DiedClosedOnPostLiveRetry asserts that when a subscription fails
// after the consumer has gone live, the consumer silently retries rather than
// propagating the failure. Died() is not closed until the consumer is explicitly
// stopped.
func TestConsumer_DiedClosedOnPostLiveRetry(t *testing.T) {
	const n = uint64(5)
	sub := newMockSub(n)
	consumer := es.NewConsumer(newMockStore(sub), &passthroughDecoder{}, &noopHandler{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	startErrCh := make(chan error, 1)
	go func() { startErrCh <- consumer.Start(ctx) }()

	// Bring consumer live.
	sendEvents(sub, n)
	select {
	case err := <-startErrCh:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Start() did not return within 2s")
	}

	// Kill the subscription — consumer retries in the background.
	sub.signalError(fmt.Errorf("nats: consumer deleted"))

	// Died() must NOT close during a retry — consumer is self-healing.
	select {
	case <-consumer.Died():
		t.Fatal("Died() closed prematurely before Stop() was called")
	case <-time.After(500 * time.Millisecond):
		t.Log("Died() has not signalled — consumer is retrying as expected ✓")
	}

	// Stop the consumer cleanly; now Died() should close.
	stopped := make(chan struct{})
	go func() {
		consumer.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop() did not return within 2s")
	}

	select {
	case <-consumer.Died():
		t.Log("Died() closed cleanly after Stop() ✓")
	default:
		t.Fatal("Died() has not closed yet after Stop() returned")
	}
}

// TestConsumer_DiedOnCleanStop asserts that after a consumer goes live and Stop()
// is called, Died() is closed so any observers unblock.
func TestConsumer_DiedOnCleanStop(t *testing.T) {
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
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Start() did not return within 2s")
	}

	stopped := make(chan struct{})
	go func() {
		consumer.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop() did not return within 2s")
	}

	select {
	case <-consumer.Died():
		t.Log("Died() closed cleanly as expected ✓")
	default:
		t.Fatal("Died() has not signalled yet after Stop() returned")
	}
}
