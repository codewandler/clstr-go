package estests

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/codewandler/clstr-go/core/es"
	"github.com/codewandler/clstr-go/core/es/estests/domain"
)

type myTestProjectionState struct {
	V int
}

func (m *myTestProjectionState) Snapshot() ([]byte, error) { return json.Marshal(m) }
func (m *myTestProjectionState) Restore(data []byte) error { return json.Unmarshal(data, m) }
func (m *myTestProjectionState) Apply(ctx context.Context, env es.Envelope, event any) (bool, error) {
	switch e := event.(type) {
	case *domain.Incremented:
		m.V += int(e.Inc)
		return true, nil
	}
	return false, nil
}

func createTestProjection(t *testing.T, s es.Snapshotter) *es.InMemoryProjection[*myTestProjectionState] {
	state := &myTestProjectionState{}
	myProj, err := es.NewInMemoryProjection[*myTestProjectionState](
		es.InMemoryProjectionOpts{
			Name:        fmt.Sprintf("%T", *new(myTestProjectionState)),
			Snapshotter: s,
			Log:         slog.Default(),
		},
		state,
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
		myCP          = es.NewInMemoryCpStore()
	)

	p := createTestProjection(t, mySnapshotter)
	te := es.NewTestEnv(
		t,
		es.WithAggregates(new(domain.TestAgg)),
		es.WithProjections(p),
		es.WithStore(myStore),
		es.WithCheckpointStore(myCP),
		es.WithSnapshotter(mySnapshotter),
	)

	repo := es.NewTypedRepositoryFrom[*domain.TestAgg](slog.Default(), te.Repository())

	// create, incBy(5), save
	a, err := repo.GetOrCreate(t.Context(), aggID)
	require.NoError(t, err)
	require.NoError(t, a.IncBy(5))
	require.Equal(t, 5, a.Count())
	require.NoError(t, repo.Save(t.Context(), a, es.WithSnapshot(true)))

	<-time.After(100 * time.Millisecond)
	require.Equal(t, 5, p.State().V)

	// next
	p = createTestProjection(t, mySnapshotter)
	te = es.NewTestEnv(
		t,
		es.WithAggregates(new(domain.TestAgg)),
		es.WithProjections(p),
		es.WithStore(myStore),
		es.WithCheckpointStore(myCP),
		es.WithSnapshotter(mySnapshotter),
	)

	repo = es.NewTypedRepositoryFrom[*domain.TestAgg](slog.Default(), te.Repository())
	a, err = repo.GetByID(t.Context(), aggID)
	require.NoError(t, err)
	require.Equal(t, 5, a.Count())
	require.NoError(t, a.IncBy(2))
	require.Equal(t, 7, a.Count())
	require.NoError(t, repo.Save(t.Context(), a, es.WithSnapshot(true)))

	<-time.After(100 * time.Millisecond)
	require.Equal(t, 7, p.State().V)
}
