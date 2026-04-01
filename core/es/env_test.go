package es_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/codewandler/clstr-go/core/es"
)

// TestEnv_ConsumerSelfHealsAfterFailure asserts that when a live consumer's
// subscription fails, the consumer retries automatically and the env stays up.
// env.Done() must NOT close — the process keeps running.
func TestEnv_ConsumerSelfHealsAfterFailure(t *testing.T) {
	const liveAt = uint64(5)

	sub1 := newMockSub(liveAt)
	sub2 := newMockSub(0) // maxSeq=0 → immediately live on subscribe (already caught up)
	store := newMockStore(sub1, sub2)

	env := es.NewEnv(
		es.WithStore(store),
		es.WithConsumer(&noopHandler{}),
	)

	startErr := make(chan error, 1)
	go func() { startErr <- env.Start() }()

	// Bring consumer live via sub1.
	sendEvents(sub1, liveAt)
	select {
	case err := <-startErr:
		require.NoError(t, err, "env.Start() should return nil once consumers are live")
	case <-time.After(2 * time.Second):
		t.Fatal("env.Start() did not return within 2s")
	}

	// Kill sub1 with a simulated infrastructure error.
	sub1.signalError(fmt.Errorf("nats: consumer deleted"))

	// env.Done() must NOT close — consumer is retrying, process stays up.
	select {
	case <-env.Done():
		t.Fatal("env.Done() closed unexpectedly — consumer should self-heal, not kill the env")
	case <-time.After(2 * time.Second):
		t.Log("env.Done() did NOT close — env is alive and consumer is self-healing ✓")
	}

	// Graceful shutdown cleans everything up.
	shutdownDone := make(chan struct{})
	go func() { env.Shutdown(); close(shutdownDone) }()
	select {
	case <-shutdownDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown() did not return within 2s")
	}
	assert.True(t, isClosed(env.Done()), "env.Done() should be closed after Shutdown()")
}

// TestEnv_WatchdogDoesNotCancelContextOnCleanStop asserts that a graceful
// env shutdown (via Shutdown()) closes env.Done() cleanly.
func TestEnv_WatchdogDoesNotCancelContextOnCleanStop(t *testing.T) {
	const liveAt = uint64(5)

	sub := newMockSub(liveAt)
	store := newMockStore(sub)

	env := es.NewEnv(
		es.WithStore(store),
		es.WithConsumer(&noopHandler{}),
	)

	startErr := make(chan error, 1)
	go func() { startErr <- env.Start() }()

	sendEvents(sub, liveAt)

	select {
	case err := <-startErr:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("env.Start() did not return within 2s")
	}

	shutdownDone := make(chan struct{})
	go func() { env.Shutdown(); close(shutdownDone) }()

	select {
	case <-shutdownDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown() did not return within 2s")
	}

	assert.True(t, isClosed(env.Done()), "env.Done() should be closed after Shutdown()")
}

// isClosed returns true if ch is already closed.
func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}
