package redis

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/codewandler/clstr-go/ports/kv"
)

func TestKV_RoundTrip(t *testing.T) {
	type payload struct {
		Name  string
		Score int
	}

	client := NewTestClient(t)
	store, err := NewKvStore(Config{Client: client, KeyPrefix: "test-rtt"})
	require.NoError(t, err)

	require.NoError(t, kv.Put[payload](t.Context(), store, "alice", payload{Name: "alice", Score: 42}, kv.PutOptions{}))

	got, err := kv.Get[payload](t.Context(), store, "alice")
	require.NoError(t, err)
	require.Equal(t, payload{Name: "alice", Score: 42}, got)
}

func TestKV_NotFound(t *testing.T) {
	client := NewTestClient(t)
	store, err := NewKvStore(Config{Client: client, KeyPrefix: "test-notfound"})
	require.NoError(t, err)

	_, err = store.Get(t.Context(), "does-not-exist")
	require.ErrorIs(t, err, kv.ErrNotFound)
}

func TestKV_TTL(t *testing.T) {
	type payload struct{ V string }

	client := NewTestClient(t)
	store, err := NewKvStore(Config{Client: client, KeyPrefix: "test-ttl"})
	require.NoError(t, err)

	ttl := 150 * time.Millisecond
	require.NoError(t, kv.Put[payload](t.Context(), store, "expiring", payload{V: "bye"}, kv.PutOptions{TTL: ttl}))

	// Key should be present immediately.
	got, err := kv.Get[payload](t.Context(), store, "expiring")
	require.NoError(t, err)
	require.Equal(t, "bye", got.V)

	// After TTL elapses the key must be gone.
	time.Sleep(ttl + 100*time.Millisecond)
	_, err = store.Get(t.Context(), "expiring")
	require.ErrorIs(t, err, kv.ErrNotFound)
}

func TestKV_Delete(t *testing.T) {
	type payload struct{ V string }

	client := NewTestClient(t)
	store, err := NewKvStore(Config{Client: client, KeyPrefix: "test-delete"})
	require.NoError(t, err)

	require.NoError(t, kv.Put[payload](t.Context(), store, "toremove", payload{V: "here"}, kv.PutOptions{}))

	_, err = kv.Get[payload](t.Context(), store, "toremove")
	require.NoError(t, err)

	require.NoError(t, store.Delete(t.Context(), "toremove"))

	_, err = store.Get(t.Context(), "toremove")
	require.ErrorIs(t, err, kv.ErrNotFound)
}

func TestKV_KeyPrefixIsolation(t *testing.T) {
	type payload struct{ V string }

	client := NewTestClient(t)

	storeA, err := NewKvStore(Config{Client: client, KeyPrefix: "ns-a"})
	require.NoError(t, err)
	storeB, err := NewKvStore(Config{Client: client, KeyPrefix: "ns-b"})
	require.NoError(t, err)

	require.NoError(t, kv.Put[payload](t.Context(), storeA, "x", payload{V: "from-a"}, kv.PutOptions{}))
	require.NoError(t, kv.Put[payload](t.Context(), storeB, "x", payload{V: "from-b"}, kv.PutOptions{}))

	gotA, err := kv.Get[payload](t.Context(), storeA, "x")
	require.NoError(t, err)
	require.Equal(t, "from-a", gotA.V)

	gotB, err := kv.Get[payload](t.Context(), storeB, "x")
	require.NoError(t, err)
	require.Equal(t, "from-b", gotB.V)
}

func TestKV_NilClientError(t *testing.T) {
	_, err := NewKvStore(Config{Client: nil})
	require.Error(t, err)
}
