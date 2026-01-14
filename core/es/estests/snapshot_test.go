package estests

import (
	"fmt"
	"log/slog"
	"testing"

	"github.com/codewandler/clstr-go/adapters/nats"
	"github.com/codewandler/clstr-go/core/es"
	"github.com/codewandler/clstr-go/core/es/estests/domain"
	gonanoid "github.com/matoous/go-nanoid/v2"
	"github.com/stretchr/testify/require"
)

func TestSnapshot(t *testing.T) {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	snapshotters := []es.Snapshotter{es.NewInMemorySnapshotter(slog.Default())}

	connectNats := nats.NewTestContainer(t)
	ss, err := nats.NewSnapshotter(nats.KvConfig{
		Bucket:  "goo",
		Connect: connectNats,
	})
	require.NoError(t, err)
	snapshotters = append(snapshotters, ss)

	store := es.NewInMemoryStore()

	for _, s := range snapshotters {
		t.Run(fmt.Sprintf("snapshotter %T", s), func(t *testing.T) {
			aggID := gonanoid.Must()
			te := es.NewTestEnv(t, es.WithStore(store), es.WithSnapshotter(s), es.WithAggregates(new(domain.TestAgg)))
			repo := es.NewTypedRepositoryFrom[*domain.TestAgg](slog.Default(), te.Repository())

			// init
			a, err := repo.GetOrCreate(t.Context(), aggID, es.WithSnapshot(true))
			require.NoError(t, err)
			require.NoError(t, a.IncBy(5))
			require.NoError(t, repo.Save(t.Context(), a, es.WithSnapshot(true)))

			// load without snapshot
			a, err = repo.GetByID(t.Context(), aggID)
			require.NoError(t, err)
			require.Equal(t, 5, a.Count())
			require.Equal(t, es.Version(2), a.GetVersion())

			// load with snapshot
			a, err = repo.GetByID(t.Context(), aggID, es.WithSnapshot(true))
			require.NoError(t, err)
			require.Equal(t, 5, a.Count())
			require.Equal(t, es.Version(2), a.GetVersion())

			// new run
			te2 := es.NewTestEnv(t, es.WithStore(store), es.WithSnapshotter(s), es.WithAggregates(new(domain.TestAgg)))
			repo = es.NewTypedRepositoryFrom[*domain.TestAgg](slog.Default(), te2.Repository())

			// load with snapshot
			a, err = repo.GetByID(t.Context(), aggID, es.WithSnapshot(true))
			require.NoError(t, err)
			require.Equal(t, 5, a.Count())
			require.Equal(t, es.Version(2), a.GetVersion())

			require.NoError(t, a.Inc())
			require.NoError(t, repo.Save(t.Context(), a, es.WithSnapshot(true)))

		})
	}
}
