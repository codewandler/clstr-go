package nats

import (
	"testing"

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
}
