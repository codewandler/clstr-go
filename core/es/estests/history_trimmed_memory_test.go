package estests

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/codewandler/clstr-go/core/es"
	"github.com/codewandler/clstr-go/core/es/estests/domain"
)

// The in-memory store is the fake consumers test against. TrimBefore makes it
// reproduce what stream retention does, so the trimmed states behave as they
// do on the NATS store.

type memTrimEnv struct {
	t     *testing.T
	store *es.InMemoryStore
	repo  es.TypedRepository[*domain.TestAgg]
}

func newMemTrimEnv(t *testing.T) *memTrimEnv {
	t.Helper()
	store := es.NewInMemoryStore()
	te := es.StartTestEnv(t,
		es.WithStore(store),
		es.WithSnapshotter(es.NewInMemorySnapshotter()),
		es.WithAggregates(new(domain.TestAgg)),
	)
	return &memTrimEnv{
		t:     t,
		store: store,
		repo:  es.NewTypedRepositoryFrom[*domain.TestAgg](slog.Default(), te.Repository()),
	}
}

func (e *memTrimEnv) create(id string) {
	e.t.Helper()
	_, err := e.repo.Create(e.t.Context(), id)
	require.NoError(e.t, err)
}

func (e *memTrimEnv) inc(id string, n int, snapshot bool) {
	e.t.Helper()
	a, err := e.repo.GetByID(e.t.Context(), id)
	require.NoError(e.t, err)
	for range n {
		require.NoError(e.t, a.Inc())
	}
	var opts []es.SaveOption
	if snapshot {
		opts = append(opts, es.WithSnapshot(true))
	}
	require.NoError(e.t, e.repo.Save(e.t.Context(), a, opts...))
}

func (e *memTrimEnv) loadFromSnapshot(id string) (*domain.TestAgg, error) {
	return e.repo.GetByID(e.t.Context(), id, es.WithSnapshot(true))
}

func TestHistoryTrimmed_InMemory_SnapshotOnly(t *testing.T) {
	e := newMemTrimEnv(t)
	e.create("a")         // v1, seq 1
	e.inc("a", 3, true)   // v2..v4, seq 2..4, snapshot v4 at seq 4
	e.create("filler")    // seq 5
	e.store.TrimBefore(5) // every event of a is gone
	w1, err := e.loadFromSnapshot("a")
	require.NoError(t, err)
	require.Equal(t, es.Version(4), w1.GetVersion())
	w2, err := e.loadFromSnapshot("a")
	require.NoError(t, err)

	// The first save after the trim continues the versions.
	require.NoError(t, w1.Inc())
	require.NoError(t, e.repo.Save(t.Context(), w1))
	require.Equal(t, es.Version(5), w1.GetVersion())

	// A second writer from the same snapshot still conflicts.
	require.NoError(t, w2.IncBy(5))
	require.ErrorIs(t, e.repo.Save(t.Context(), w2), es.ErrConcurrencyConflict)

	a, err := e.loadFromSnapshot("a")
	require.NoError(t, err)
	require.Equal(t, es.Version(5), a.GetVersion())
	require.Equal(t, 4, a.Count(), "the first writer's event is the one stored")

	// A full replay cannot rebuild it.
	_, err = e.repo.GetByID(t.Context(), "a")
	requireHistoryTrimmed(t, err, es.HistoryTrimmedError{
		AggType:            "test_agg",
		AggID:              "a",
		StartVersion:       1,
		StartSeq:           1,
		FirstStoredVersion: 5,
		FirstStoredSeq:     6,
		StreamFirstSeq:     5,
	})
}

func TestHistoryTrimmed_InMemory_GapAfterSnapshot(t *testing.T) {
	e := newMemTrimEnv(t)
	e.create("a")        // v1, seq 1
	e.inc("a", 1, true)  // v2, seq 2, snapshot v2 at seq 2
	e.inc("a", 2, false) // v3, v4 at seq 3, 4
	e.store.TrimBefore(4)

	_, err := e.loadFromSnapshot("a")
	requireHistoryTrimmed(t, err, es.HistoryTrimmedError{
		AggType:            "test_agg",
		AggID:              "a",
		StartVersion:       3,
		StartSeq:           3,
		FirstStoredVersion: 4,
		FirstStoredSeq:     4,
		StreamFirstSeq:     4,
	})
}

func TestHistoryTrimmed_InMemory_EmptySubjectWithoutTrimProofConflicts(t *testing.T) {
	ctx := t.Context()

	t.Run("no last known sequence", func(t *testing.T) {
		e := newMemTrimEnv(t)
		e.create("a")
		e.inc("a", 3, true)
		e.create("filler")
		e.store.TrimBefore(5)

		_, err := es.AppendEvents(ctx, e.store, "test_agg", "a", 4, &domain.Incremented{Inc: 1})
		require.ErrorIs(t, err, es.ErrConcurrencyConflict)
	})

	t.Run("last known sequence inside the retained range", func(t *testing.T) {
		e := newMemTrimEnv(t)
		e.create("a")
		e.inc("a", 3, true)
		e.create("filler")
		e.store.TrimBefore(5)

		_, err := e.store.Append(ctx, "test_agg", "a", 4,
			[]es.Envelope{incEnvelope("a", 5)}, es.WithLastKnownSeq(5))
		require.ErrorIs(t, err, es.ErrConcurrencyConflict)
	})
}
