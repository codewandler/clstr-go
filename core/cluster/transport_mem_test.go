package cluster

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTransport_Memory(t *testing.T) {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	tr := NewInMemoryTransport()
	rcv := make(chan Envelope, 1)
	s, err := tr.SubscribeShard(t.Context(), 5, func(ctx context.Context, envelope Envelope) ([]byte, error) {
		rcv <- envelope
		return nil, nil
	})
	require.NoError(t, err)
	require.NotNil(t, s)

	_, err = tr.Request(t.Context(), Envelope{
		Shard: 5,
		Data:  []byte("hello"),
		Type:  "banana",
	})
	require.NoError(t, err)

	select {
	case <-time.After(10 * time.Millisecond):
		t.Fatal("no message received")
	case env := <-rcv:
		require.Equal(t, "hello", string(env.Data))
	}

	require.NoError(t, s.Unsubscribe())
	require.NoError(t, tr.Close())

}

func TestTransport_Memory_publish_error(t *testing.T) {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	tr := NewInMemoryTransport().WithLog(slog.Default())
	s, err := tr.SubscribeShard(t.Context(), 5, func(ctx context.Context, envelope Envelope) ([]byte, error) {
		return nil, errors.New("boom")
	})
	require.NoError(t, err)
	require.NotNil(t, s)

	_, err = tr.Request(t.Context(), Envelope{
		Shard: 5,
		Data:  []byte("hello"),
		Type:  "banana",
	})
	require.ErrorContains(
		t,
		err,
		"boom",
	)

	<-time.After(10 * time.Millisecond)

	require.NoError(t, s.Unsubscribe())
	require.NoError(t, tr.Close())

}

func TestTransport_Memory_TTL_Expired(t *testing.T) {
	tr := NewInMemoryTransport()
	_, err := tr.SubscribeShard(t.Context(), 5, func(ctx context.Context, envelope Envelope) ([]byte, error) {
		return []byte("ok"), nil
	})
	require.NoError(t, err)

	// Send with already-expired TTL
	_, err = tr.Request(t.Context(), Envelope{
		Shard:       5,
		Data:        []byte("hello"),
		Type:        "test",
		TTLMs:       100,
		CreatedAtMs: time.Now().UnixMilli() - 200, // 200ms ago
	})
	require.ErrorIs(t, err, ErrEnvelopeExpired)

	require.NoError(t, tr.Close())
}

func TestTransport_Memory_TTL_AutoSet(t *testing.T) {
	tr := NewInMemoryTransport()
	receivedEnv := make(chan Envelope, 1)
	_, err := tr.SubscribeShard(t.Context(), 5, func(ctx context.Context, envelope Envelope) ([]byte, error) {
		receivedEnv <- envelope
		return []byte("ok"), nil
	})
	require.NoError(t, err)

	// Send with TTL but no CreatedAtMs - transport should set it
	_, err = tr.Request(t.Context(), Envelope{
		Shard: 5,
		Data:  []byte("hello"),
		Type:  "test",
		TTLMs: 5000,
	})
	require.NoError(t, err)

	select {
	case env := <-receivedEnv:
		require.Greater(t, env.CreatedAtMs, int64(0))
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout waiting for envelope")
	}

	require.NoError(t, tr.Close())
}

func TestTransport_Memory_HandlerTimeout(t *testing.T) {
	tr := NewInMemoryTransport(MemoryTransportOpts{
		HandlerTimeout: 50 * time.Millisecond,
	})

	_, err := tr.SubscribeShard(t.Context(), 5, func(ctx context.Context, envelope Envelope) ([]byte, error) {
		// Simulate slow handler
		select {
		case <-time.After(200 * time.Millisecond):
			return []byte("ok"), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	require.NoError(t, err)

	_, err = tr.Request(t.Context(), Envelope{
		Shard: 5,
		Data:  []byte("hello"),
		Type:  "test",
	})
	require.ErrorContains(t, err, ErrHandlerTimeout.Error())

	require.NoError(t, tr.Close())
}

func TestTransport_Memory_ConcurrencyLimit(t *testing.T) {
	maxConcurrent := 2
	tr := NewInMemoryTransport(MemoryTransportOpts{
		MaxConcurrentHandlers: maxConcurrent,
		HandlerTimeout:        5 * time.Second,
	})

	var (
		activeCount  int
		maxActive    int
		mu           sync.Mutex
		handlerDone  = make(chan struct{}, 10)
		handlerStart = make(chan struct{}, 10)
	)

	_, err := tr.SubscribeShard(t.Context(), 5, func(ctx context.Context, envelope Envelope) ([]byte, error) {
		mu.Lock()
		activeCount++
		if activeCount > maxActive {
			maxActive = activeCount
		}
		mu.Unlock()

		handlerStart <- struct{}{}

		// Hold for a bit
		time.Sleep(50 * time.Millisecond)

		mu.Lock()
		activeCount--
		mu.Unlock()

		handlerDone <- struct{}{}
		return []byte("ok"), nil
	})
	require.NoError(t, err)

	// Send multiple requests concurrently
	numRequests := 5
	results := make(chan error, numRequests)

	for range numRequests {
		go func() {
			_, err := tr.Request(t.Context(), Envelope{
				Shard: 5,
				Data:  []byte("hello"),
				Type:  "test",
			})
			results <- err
		}()
	}

	// Wait for all requests to complete
	for range numRequests {
		err := <-results
		require.NoError(t, err)
	}

	// Verify max concurrency was respected
	mu.Lock()
	actualMax := maxActive
	mu.Unlock()

	require.LessOrEqual(t, actualMax, maxConcurrent)

	require.NoError(t, tr.Close())
}

func TestTransport_Memory_ReservedHeaderRejected(t *testing.T) {
	tr := NewInMemoryTransport()
	_, err := tr.SubscribeShard(t.Context(), 5, func(ctx context.Context, envelope Envelope) ([]byte, error) {
		return []byte("ok"), nil
	})
	require.NoError(t, err)

	// Request with reserved header should be rejected
	_, err = tr.Request(t.Context(), Envelope{
		Shard: 5,
		Data:  []byte("hello"),
		Type:  "test",
		Headers: map[string]string{
			"x-clstr-internal": "bad",
		},
	})
	require.ErrorIs(t, err, ErrReservedHeader)

	require.NoError(t, tr.Close())
}

func TestTransport_Memory_GracefulShutdown(t *testing.T) {
	tr := NewInMemoryTransport(MemoryTransportOpts{
		HandlerTimeout: 5 * time.Second,
	})

	handlerStarted := make(chan struct{})
	handlerDone := make(chan struct{})

	_, err := tr.SubscribeShard(t.Context(), 5, func(ctx context.Context, envelope Envelope) ([]byte, error) {
		close(handlerStarted)
		time.Sleep(100 * time.Millisecond)
		close(handlerDone)
		return []byte("ok"), nil
	})
	require.NoError(t, err)

	// Start a request
	go func() {
		_, _ = tr.Request(context.Background(), Envelope{
			Shard: 5,
			Data:  []byte("hello"),
			Type:  "test",
		})
	}()

	// Wait for handler to start
	<-handlerStarted

	// Close should wait for handler to finish
	closeStart := time.Now()
	require.NoError(t, tr.Close())
	closeDuration := time.Since(closeStart)

	// Verify close waited for handler
	select {
	case <-handlerDone:
		// Handler completed
	default:
		t.Fatal("Close() returned before handler completed")
	}

	require.Greater(t, closeDuration, 50*time.Millisecond)
}
