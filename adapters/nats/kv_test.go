package nats

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKV(t *testing.T) {
	type fooBar struct {
		Fruit string
		Count int
	}
	connectNats := NewTestContainer(t)
	kv, err := NewKvStore[fooBar](KvConfig{
		Bucket:  "fruits",
		Connect: connectNats,
	})
	require.NoError(t, err)
	require.NoError(t, kv.Set(t.Context(), "apple", fooBar{Fruit: "apple", Count: 10}))

	v, err := kv.Get(t.Context(), "apple")
	require.NoError(t, err)
	require.Equal(t, fooBar{Fruit: "apple", Count: 10}, v)
}
