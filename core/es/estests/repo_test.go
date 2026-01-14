package estests

import (
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/codewandler/clstr-go/core/es"
	"github.com/codewandler/clstr-go/core/es/estests/domain"
	"github.com/stretchr/testify/require"
)

func TestRepository(t *testing.T) {
	te := es.NewTestEnv(t, es.WithAggregates(new(domain.TestAgg)))
	a := domain.NewTestAgg("foobar")
	require.ErrorIs(t, te.Repository().Load(t.Context(), a), es.ErrAggregateNotFound)
}

func TestRepository_Typed_notFound(t *testing.T) {
	te := es.NewTestEnv(t, es.WithAggregates(new(domain.TestAgg)))
	r := es.NewTypedRepositoryFrom[*domain.TestAgg](slog.Default(), te.Repository())
	_, err := r.GetByID(t.Context(), "foobar")
	require.ErrorIs(t, err, es.ErrAggregateNotFound)
}

func TestRepository_Typed(t *testing.T) {
	var (
		e    = es.NewTestEnv(t, es.WithAggregates(new(domain.TestAgg)))
		repo = es.NewTypedRepositoryFrom[*domain.TestAgg](slog.Default(), e.Repository())
	)

	var (
		aggID = "my-agg-1"
	)

	slog.SetLogLoggerLevel(slog.LevelDebug)

	require.Equal(t, "test_agg", repo.GetAggType())

	// init
	a := repo.NewWithID(aggID)
	require.Equal(t, aggID, a.GetID())
	require.EqualValues(t, 0, a.GetVersion())

	// load fails
	require.ErrorIs(t, repo.Load(t.Context(), a), es.ErrAggregateNotFound)

	// save
	require.NoError(t, a.IncBy(7))
	require.NoError(t, repo.Save(t.Context(), a))
	require.EqualValues(t, 1, a.GetVersion())
	require.EqualValues(t, 7, a.Count())

	t.Run("load", func(t *testing.T) {
		// init
		loaded := repo.NewWithID(aggID)
		require.Equal(t, aggID, loaded.GetID())
		require.Equal(t, uint64(0), loaded.GetSeq())
		require.EqualValues(t, 0, loaded.GetVersion())

		// load
		require.NoError(t, repo.Load(t.Context(), loaded))
		t.Logf("loaded: %+v", loaded)
		require.EqualValues(t, 7, loaded.Count())
		require.EqualValues(t, 1, loaded.GetVersion())
	})

	t.Run("load by ID", func(t *testing.T) {
		loaded2, err := repo.GetByID(t.Context(), "my-agg-1")
		require.NoError(t, err)
		require.NotNil(t, loaded2)
		//require.Equal(t, loaded, loaded2)
		require.Equal(t, 7, loaded2.Count())

		d, _ := json.MarshalIndent(loaded2, "", "  ")
		println(string(d))
	})

}
