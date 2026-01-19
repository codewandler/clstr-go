package es

import (
	"encoding/json"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type myProjTestEvent struct {
	Value int
}

type (
	myInMemoryTestProj struct {
		mu      sync.RWMutex
		Counter int
	}
)

func (m *myInMemoryTestProj) GetValue() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.Counter
}
func (m *myInMemoryTestProj) Name() string                      { return "test_proj" }
func (m *myInMemoryTestProj) Snapshot() ([]byte, error)         { return json.Marshal(m) }
func (m *myInMemoryTestProj) RestoreSnapshot(data []byte) error { return json.Unmarshal(data, m) }
func (m *myInMemoryTestProj) Handle(msgCtx MsgCtx) error {
	event := msgCtx.Event()
	switch e := event.(type) {
	case *myProjTestEvent:
		m.mu.Lock()
		defer m.mu.Unlock()
		m.Counter += e.Value
		return nil
	}
	return nil
}

func TestProjection_InMemory(t *testing.T) {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	var (
		memSnapshotter = NewInMemorySnapshotter()
		store          = NewInMemoryStore()
	)

	newP := func() *SnapshotProjection[*myInMemoryTestProj] {
		p := &myInMemoryTestProj{
			Counter: 0,
		}
		sp, err := NewSnapshotProjection(slog.Default(), p, memSnapshotter)
		require.NoError(t, err)
		return sp
	}

	// === initial first run ===

	p := newP()
	require.NotNil(t, p)
	require.Equal(t, Version(0), p.persistedProjectionVersion)
	require.Equal(t, uint64(0), p.persistedLastSeq)
	require.Equal(t, 0, p.Projection().GetValue())
	require.Equal(t, "test_proj", p.Name())

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
	require.Equal(t, 60, p.Projection().GetValue())

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

	println("---NEW---")

	p = newP()
	require.NotNil(t, p)
	require.Equal(t, 1, int(p.persistedProjectionVersion))
	ls, _ = p.GetLastSeq()
	require.Equal(t, uint64(10), ls)
	require.Equal(t, 50, p.Projection().GetValue())

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
	require.Equal(t, 65, p.Projection().GetValue())
	// TODO: seq, version

}
