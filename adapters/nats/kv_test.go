package nats

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/codewandler/clstr-go/ports/kv"
)

func TestKV(t *testing.T) {
	type fooBar struct {
		Fruit string
		Count int
	}
	connectNats := NewTestContainer(t)
	kvStore, err := NewKvStore(KvConfig{
		Bucket:  "fruits",
		Connect: connectNats,
	})
	require.NoError(t, err)
	require.NoError(t, kv.Put[fooBar](t.Context(), kvStore, "apple", fooBar{Fruit: "apple", Count: 10}, kv.PutOptions{}))

	v, err := kv.Get[fooBar](t.Context(), kvStore, "apple")
	require.NoError(t, err)
	require.Equal(t, fooBar{Fruit: "apple", Count: 10}, v)

	// TTL
	ttl := 1 * time.Second
	require.NoError(t, kv.Put[fooBar](t.Context(), kvStore, "banana", fooBar{Fruit: "banana", Count: 20}, kv.PutOptions{TTL: ttl}))
	<-time.After(ttl + 100*time.Millisecond)
	_, err = kv.Get[fooBar](t.Context(), kvStore, "banana")
	require.ErrorIs(t, err, kv.ErrNotFound)
}
