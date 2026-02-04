package cluster

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestScopedHandler_LRU_Eviction(t *testing.T) {
	cacheSize := 3
	var createCount atomic.Int32

	h := NewScopedHandler(ScopedHandlerOpts{
		Extract: func(env Envelope) (string, error) {
			key, _ := env.GetHeader(envHeaderKey)
			return key, nil
		},
		Create: func(key string) (ServerHandlerFunc, error) {
			createCount.Add(1)
			return func(ctx context.Context, env Envelope) ([]byte, error) {
				return []byte(key), nil
			}, nil
		},
		CacheSize: cacheSize,
	})

	ctx := context.Background()

	// Call with keys 1, 2, 3 - all should create handlers
	for _, key := range []string{"key1", "key2", "key3"} {
		_, err := h(ctx, Envelope{Headers: map[string]string{envHeaderKey: key}})
		require.NoError(t, err)
	}
	require.Equal(t, int32(3), createCount.Load())

	// Call key1 again - should be cached (no new create)
	_, err := h(ctx, Envelope{Headers: map[string]string{envHeaderKey: "key1"}})
	require.NoError(t, err)
	require.Equal(t, int32(3), createCount.Load())

	// Call key4 - should evict LRU (key2) and create new
	_, err = h(ctx, Envelope{Headers: map[string]string{envHeaderKey: "key4"}})
	require.NoError(t, err)
	require.Equal(t, int32(4), createCount.Load())

	// Call key2 again - was evicted, should create new
	_, err = h(ctx, Envelope{Headers: map[string]string{envHeaderKey: "key2"}})
	require.NoError(t, err)
	require.Equal(t, int32(5), createCount.Load())
}

func TestScopedHandler_Unbounded(t *testing.T) {
	var createCount atomic.Int32

	h := NewScopedHandler(ScopedHandlerOpts{
		Extract: func(env Envelope) (string, error) {
			key, _ := env.GetHeader(envHeaderKey)
			return key, nil
		},
		Create: func(key string) (ServerHandlerFunc, error) {
			createCount.Add(1)
			return func(ctx context.Context, env Envelope) ([]byte, error) {
				return []byte(key), nil
			}, nil
		},
		CacheSize: -1, // Unlimited
	})

	ctx := context.Background()

	// Create many keys
	for i := range 100 {
		key := string(rune('A' + i%26))
		_, err := h(ctx, Envelope{Headers: map[string]string{envHeaderKey: key}})
		require.NoError(t, err)
	}

	// Should have created 26 unique handlers (A-Z)
	require.Equal(t, int32(26), createCount.Load())

	// Call again - all should be cached
	for i := range 100 {
		key := string(rune('A' + i%26))
		_, err := h(ctx, Envelope{Headers: map[string]string{envHeaderKey: key}})
		require.NoError(t, err)
	}
	require.Equal(t, int32(26), createCount.Load())
}

func TestScopedHandler_EmptyKey(t *testing.T) {
	h := NewScopedHandler(ScopedHandlerOpts{
		Extract: func(env Envelope) (string, error) {
			return "", nil // Return empty key
		},
		Create: func(key string) (ServerHandlerFunc, error) {
			return func(ctx context.Context, env Envelope) ([]byte, error) {
				return nil, nil
			}, nil
		},
	})

	_, err := h(context.Background(), Envelope{})
	require.ErrorIs(t, err, ErrKeyRequired)
}

func TestNewKeyHandler_MissingHeader(t *testing.T) {
	h := NewKeyHandler(func(key string) (ServerHandlerFunc, error) {
		return func(ctx context.Context, env Envelope) ([]byte, error) {
			return nil, nil
		}, nil
	})

	_, err := h(context.Background(), Envelope{})
	require.ErrorIs(t, err, ErrMissingKeyHeader)
}

func TestNewKeyHandlerWithOpts_CacheSize(t *testing.T) {
	var createCount atomic.Int32

	h := NewKeyHandlerWithOpts(func(key string) (ServerHandlerFunc, error) {
		createCount.Add(1)
		return func(ctx context.Context, env Envelope) ([]byte, error) {
			return []byte(key), nil
		}, nil
	}, 2) // Cache size of 2

	ctx := context.Background()

	// Use 3 different keys
	for _, key := range []string{"a", "b", "c"} {
		_, err := h(ctx, Envelope{Headers: map[string]string{envHeaderKey: key}})
		require.NoError(t, err)
	}
	require.Equal(t, int32(3), createCount.Load())

	// First key should have been evicted
	_, err := h(ctx, Envelope{Headers: map[string]string{envHeaderKey: "a"}})
	require.NoError(t, err)
	require.Equal(t, int32(4), createCount.Load())
}
