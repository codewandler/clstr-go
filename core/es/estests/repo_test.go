package estests

import (
	"encoding/json"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/codewandler/clstr-go/core/es"
	"github.com/codewandler/clstr-go/core/es/estests/domain"
)

func TestRepository(t *testing.T) {
	te := es.StartTestEnv(t, es.WithAggregates(new(domain.TestAgg)))
	a := domain.NewTestAgg("foobar")
	require.ErrorIs(t, te.Repository().Load(t.Context(), a), es.ErrAggregateNotFound)
}

func TestRepository_Typed_notFound(t *testing.T) {
	te := es.StartTestEnv(t, es.WithAggregates(new(domain.TestAgg)))
	r := es.NewTypedRepositoryFrom[*domain.TestAgg](slog.Default(), te.Repository())
	_, err := r.GetByID(t.Context(), "foobar")
	require.ErrorIs(t, err, es.ErrAggregateNotFound)
}

func TestRepository_Typed(t *testing.T) {
	var (
		e    = es.StartTestEnv(t, es.WithAggregates(new(domain.TestAgg)))
		repo = es.NewTypedRepositoryFrom[*domain.TestAgg](slog.Default(), e.Repository())
	)

	var (
		aggID = "my-agg-1"
	)

	slog.SetLogLoggerLevel(slog.LevelDebug)

	require.Equal(t, "test_agg", repo.GetAggType())

	// load fails
	_, err := repo.GetByID(t.Context(), aggID)
	require.ErrorIs(t, err, es.ErrAggregateNotFound)

	a, err := repo.Create(t.Context(), aggID)
	require.NoError(t, err)
	require.NotNil(t, a)
	require.Equal(t, aggID, a.GetID())
	require.EqualValues(t, es.Version(1), a.GetVersion())

	// save
	require.NoError(t, a.IncBy(7))
	require.NoError(t, repo.Save(t.Context(), a))
	require.EqualValues(t, 2, a.GetVersion())
	require.EqualValues(t, 7, a.Count())

	t.Run("load", func(t *testing.T) {
		// init
		var loaded *domain.TestAgg
		loaded, err = repo.GetByID(t.Context(), aggID)
		require.NoError(t, err)
		require.Equal(t, aggID, loaded.GetID())
		require.EqualValues(t, 7, loaded.Count())
		require.EqualValues(t, 2, loaded.GetVersion())

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

func TestRepository_Concurrency(t *testing.T) {
	te := es.StartTestEnv(t, es.WithAggregates(new(domain.TestAgg)))
	r := es.NewTypedRepositoryFrom[*domain.TestAgg](slog.Default(), te.Repository(), es.WithRepoCacheLRU(100))

	t.Run("transactions", func(t *testing.T) {
		a, err := r.Create(t.Context(), "my-agg-1")
		require.NoError(t, err)
		require.NotNil(t, a)

		var N = 10
		var wg sync.WaitGroup
		wg.Add(N)
		for i := 0; i < 10; i++ {
			go func() {
				assert.NoError(t, r.WithTransaction(t.Context(), "my-agg-1", func(b *domain.TestAgg) (err error) {
					require.NoError(t, b.IncBy(1))
					return nil
				}))
				wg.Done()
			}()
		}

		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-time.After(time.Second * 2):
			t.Fatal("timeout")
		case <-done:
		}

		a, err = r.GetByID(t.Context(), "my-agg-1")
		require.NoError(t, err)
		require.EqualValues(t, 10, a.Count())
	})

}
