package es

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
)

type (
	myInMemoryTestProj struct {
		Counter int
	}
)

func (m *myInMemoryTestProj) Snapshot() ([]byte, error) { return json.Marshal(m) }
func (m *myInMemoryTestProj) Restore(data []byte) error { return json.Unmarshal(data, m) }
func (m *myInMemoryTestProj) Apply(ctx context.Context, env Envelope, event any) (bool, error) {
	switch e := event.(type) {
	case int:
		m.Counter += e
		return true, nil
	}
	return false, nil
}

func TestProjection_InMemory(t *testing.T) {
	slog.SetLogLoggerLevel(slog.LevelDebug)
	memSnapshotter := NewInMemorySnapshotter(slog.Default())

	type MyProj = InMemoryProjection[*myInMemoryTestProj]

	newP := func() *MyProj {
		p, err := NewInMemoryProjection[*myInMemoryTestProj](
			InMemoryProjectionOpts{
				Name:        "foobar",
				Snapshotter: memSnapshotter,
			},
			&myInMemoryTestProj{
				Counter: 1,
			},
		)
		require.NoError(t, err)
		return p
	}

	p := newP()
	require.NotNil(t, p)

	require.NoError(t, p.Handle(t.Context(), Envelope{Type: "foo"}, 5))
	require.Equal(t, 6, p.State().Counter)

	p2 := newP()
	require.Equal(t, 6, p2.State().Counter)
}
