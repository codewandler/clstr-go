package es

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type myProjTestEvent struct {
	Value int
}

type (
	myInMemoryTestProj struct {
		Counter int
	}
)

func (m *myInMemoryTestProj) Snapshot() ([]byte, error) { return json.Marshal(m) }
func (m *myInMemoryTestProj) Restore(data []byte) error { return json.Unmarshal(data, m) }
func (m *myInMemoryTestProj) Apply(ctx context.Context, env Envelope, event any) (bool, error) {
	switch e := event.(type) {
	case *myProjTestEvent:
		m.Counter += e.Value
		return true, nil
	}
	return false, nil
}

type testMemoryProj = InMemoryProjection[*myInMemoryTestProj]

func TestProjection_InMemory(t *testing.T) {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	var (
		memSnapshotter = NewInMemorySnapshotter()
		store          = NewInMemoryStore()
	)

	newP := func() *testMemoryProj {
		p, err := NewInMemoryProjection[*myInMemoryTestProj](
			InMemoryProjectionOpts{
				Name:        "foobar",
				Snapshotter: memSnapshotter,
			},
			&myInMemoryTestProj{
				Counter: 0,
			},
		)
		require.NoError(t, err)
		return p
	}

	// === initial first run ===

	p := newP()
	require.NotNil(t, p)

	te := NewTestEnv(
		t,
		WithCtx(t.Context()),
		WithStore(store),

		WithProjection(p),
		WithEvent[myProjTestEvent](),
	)

	for i := 0; i < 12; i++ {
		te.Assert().Append(
			t.Context(),
			Version(i),
			"foo",
			"bar",
			myProjTestEvent{Value: 5},
		)
	}

	<-time.After(50 * time.Millisecond)
	require.Equal(t, 60, p.State().Counter)

	ls, _ := p.GetLastSeq()
	require.Equal(t, uint64(10), ls)
	require.Equal(t, 1, int(p.persistedProjectionVersion))
	te.Shutdown()

	// TODO: configure how often snapshot is being created
	// TODO: maybe snapshots should be done in background, periodically

	/*p2 := newP()
	require.Equal(t, 60, p2.State().Counter)*/

	// TODO: start a new env, make sure projection is loaded from snapshot
	// and that events from checkpoint store are applied from the right offset

	p = newP()
	require.NotNil(t, p)
	require.Equal(t, 1, int(p.persistedProjectionVersion))
	ls, _ = p.GetLastSeq()
	require.Equal(t, uint64(10), ls)
	require.Equal(t, 50, p.State().Counter)

	te = NewTestEnv(
		t,
		WithStore(store),
		WithProjection(p),
		WithEvent[myProjTestEvent](),
	)

	require.NoError(t, te.Append(
		t.Context(),
		Version(12),
		"foo",
		"bar",
		myProjTestEvent{Value: 5},
	))

	<-time.After(1500 * time.Millisecond)
	require.Equal(t, 65, p.State().Counter)
	// TODO: seq, version

}
