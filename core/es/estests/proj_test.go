package estests

import (
	"encoding/json"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/codewandler/clstr-go/core/es"
	"github.com/codewandler/clstr-go/core/es/estests/domain"
)

type myTestProjection struct {
	mu sync.RWMutex
	V  int
}

func (m *myTestProjection) getValue() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.V
}

func (m *myTestProjection) Name() string                      { return "test_projection" }
func (m *myTestProjection) Snapshot() ([]byte, error)         { return json.Marshal(m) }
func (m *myTestProjection) RestoreSnapshot(data []byte) error { return json.Unmarshal(data, m) }
func (m *myTestProjection) Handle(msgCtx es.MsgCtx) error {
	switch e := msgCtx.Event().(type) {
	case *domain.Incremented:
		m.mu.Lock()
		defer m.mu.Unlock()
		m.V += int(e.Inc)
		return nil
	}
	return nil
}

func createTestProjection(t *testing.T, s es.Snapshotter) *es.SnapshotProjection[*myTestProjection] {
	pInner := &myTestProjection{}
	myProj, err := es.NewSnapshotProjection[*myTestProjection](
		pInner,
		s,
	)
	require.NoError(t, err)
	return myProj
}

func TestProjection(t *testing.T) {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	var (
		aggID         = "my-agg-1"
		mySnapshotter = es.NewInMemorySnapshotter()
		myStore       = es.NewInMemoryStore()
	)

	mainProj := createTestProjection(t, mySnapshotter)
	te := es.NewTestEnv(
		t,
		es.WithLog(slog.Default()),
		es.WithAggregates(new(domain.TestAgg)),
		es.WithProjection(mainProj),
		es.WithStore(myStore),
		es.WithSnapshotter(mySnapshotter),
	)

	repo := es.NewTypedRepositoryFrom[*domain.TestAgg](slog.Default(), te.Repository())

	// create, incBy(5), save
	a, err := repo.GetOrCreate(t.Context(), aggID)
	require.NoError(t, err)
	require.NoError(t, a.IncBy(5))
	require.Equal(t, 5, a.Count())
	require.NoError(t, repo.Save(t.Context(), a, es.WithSnapshot(true)))

	<-time.After(50 * time.Millisecond)
	require.Equal(t, 5, mainProj.Projection().getValue())
	te.Shutdown()
	<-time.After(40 * time.Millisecond)
	require.Equal(t, 5, mainProj.Projection().getValue())

	// next
	mainProj = createTestProjection(t, mySnapshotter)
	te = es.NewTestEnv(
		t,
		es.WithAggregates(new(domain.TestAgg)),
		es.WithProjection(mainProj),
		es.WithStore(myStore),
		es.WithSnapshotter(mySnapshotter),
	)

	require.Equal(t, 5, mainProj.Projection().getValue())

	repo = es.NewTypedRepositoryFrom[*domain.TestAgg](slog.Default(), te.Repository())
	a, err = repo.GetByID(t.Context(), aggID)
	require.NoError(t, err)
	require.Equal(t, 5, a.Count())

	require.NoError(t, a.IncBy(2))
	require.Equal(t, 7, a.Count())
	require.NoError(t, repo.Save(t.Context(), a, es.WithSnapshot(true)))

	<-time.After(50 * time.Millisecond)
	require.Equal(t, 7, mainProj.Projection().getValue())
}
