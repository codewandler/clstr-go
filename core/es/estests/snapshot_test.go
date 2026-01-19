package estests

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"testing"

	gonanoid "github.com/matoous/go-nanoid/v2"
	"github.com/stretchr/testify/require"

	"github.com/codewandler/clstr-go/adapters/nats"
	"github.com/codewandler/clstr-go/core/es"
	"github.com/codewandler/clstr-go/core/es/estests/domain"
)

func TestSnapshot(t *testing.T) {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	connectNats := nats.NewTestContainer(t)

	// SUT: snapshotters
	natsSnapshotter, err := nats.NewSnapshotter(nats.KvConfig{
		Bucket:  "goo",
		Connect: connectNats,
	})
	require.NoError(t, err)
	snapshotters := []es.Snapshotter{
		es.NewInMemorySnapshotter(),
		natsSnapshotter,
	}

	// test each snapshotter
	for _, s := range snapshotters {
		t.Run(fmt.Sprintf("snapshotter %T", s), func(t *testing.T) {
			store := es.NewInMemoryStore()

			aggID := gonanoid.Must()
			te := es.StartTestEnv(t, es.WithStore(store), es.WithSnapshotter(s), es.WithAggregates(new(domain.TestAgg)))
			repo := es.NewTypedRepositoryFrom[*domain.TestAgg](slog.Default(), te.Repository())

			// init
			a, err := repo.GetOrCreate(t.Context(), aggID, es.WithSnapshot(true))
			require.NoError(t, err)
			require.NoError(t, a.IncBy(5))
			require.NoError(t, a.IncBy(5))
			require.NoError(t, a.IncBy(5))
			require.NoError(t, repo.Save(t.Context(), a, es.WithSnapshot(true)))

			ss, err := es.LoadSnapshot(t.Context(), s, a.GetAggType(), a.GetID())
			require.NoError(t, err)
			require.NotNil(t, ss)
			dd, _ := json.MarshalIndent(ss, "", "  ")
			println(string(dd))

			// load without snapshot
			a, err = repo.GetByID(t.Context(), aggID)
			require.NoError(t, err)
			require.Equal(t, 15, a.Count())
			require.Equal(t, es.Version(4), a.GetVersion())

			// load with snapshot
			a, err = repo.GetByID(t.Context(), aggID, es.WithSnapshot(true))
			require.NoError(t, err)
			require.Equal(t, 15, a.Count())
			require.Equal(t, es.Version(4), a.GetVersion())

			// new run
			te2 := es.StartTestEnv(t, es.WithStore(store), es.WithSnapshotter(s), es.WithAggregates(new(domain.TestAgg)))
			repo = es.NewTypedRepositoryFrom[*domain.TestAgg](slog.Default(), te2.Repository())

			// load with snapshot
			a, err = repo.GetByID(t.Context(), aggID, es.WithSnapshot(true))
			require.NoError(t, err)
			require.Equal(t, 15, a.Count())
			require.Equal(t, es.Version(4), a.GetVersion())

			require.NoError(t, a.Inc())
			require.NoError(t, repo.Save(t.Context(), a, es.WithSnapshot(true)))

			a, err = repo.GetOrCreate(t.Context(), aggID, es.WithSnapshot(true))
			require.NoError(t, err)
			require.Equal(t, 16, a.Count())
			require.Equal(t, es.Version(5), a.GetVersion(), "version correct")
			require.EqualValues(t, uint64(5), a.GetSeq(), "seq correct")

		})
	}
}
