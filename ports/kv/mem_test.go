package kv

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_Memory(t *testing.T) {
	type Foo struct {
		Name string
		Age  int
	}
	s := NewMemStore()

	_, err := Get[Foo](t.Context(), s, "foobar")
	require.ErrorIs(t, err, ErrNotFound)

	require.NoError(t, Put[Foo](t.Context(), s, "p1", Foo{Name: "P1", Age: 10}, PutOptions{}))
	require.NoError(t, Put[Foo](t.Context(), s, "p2", Foo{Name: "P2", Age: 20}, PutOptions{}))

	loaded, err := Get[Foo](t.Context(), s, "p1")
	require.NoError(t, err)
	require.Equal(t, Foo{Name: "P1", Age: 10}, loaded)

	require.NoError(t, s.Delete(t.Context(), "p1"))
	_, err = Get[Foo](t.Context(), s, "p1")
	require.ErrorIs(t, err, ErrNotFound)
}
