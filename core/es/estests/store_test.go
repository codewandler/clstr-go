package estests

import (
	"log/slog"
	"testing"
	"time"

	gonanoid "github.com/matoous/go-nanoid/v2"
	"github.com/stretchr/testify/require"

	"github.com/codewandler/clstr-go/adapters/nats"
	"github.com/codewandler/clstr-go/core/es"
	"github.com/codewandler/clstr-go/core/es/estests/domain"
)

type testCase struct {
	name        string
	store       es.EventStore
	snapshotter es.Snapshotter
}

func getStoreSUTs(t *testing.T) []testCase {
	var (
		streamSubjects = []string{"clstr.es.>"}
		subjectPrefix  = "clstr.es.tenant-1"
	)

	return []testCase{
		{
			name:        "1. ALL memory",
			store:       es.NewInMemoryStore(),
			snapshotter: es.NewInMemorySnapshotter(),
		},
		func() testCase {
			connectNatsC := nats.NewTestContainer(t)
			natsES, err := nats.NewEventStore(nats.EventStoreConfig{
				Log:            slog.Default(),
				Connect:        connectNatsC,
				StreamSubjects: streamSubjects,
				SubjectPrefix:  subjectPrefix,
			})
			require.NoError(t, err)
			require.NotNil(t, natsES)

			natsSnapshotter, err := nats.NewSnapshotter(nats.KvConfig{
				Connect: connectNatsC,
				Bucket:  "foobar",
			})
			require.NoError(t, err)
			require.NotNil(t, natsSnapshotter)

			return testCase{
				name:        "2. ALL nats",
				store:       natsES,
				snapshotter: natsSnapshotter,
			}
		}(),
		func() testCase {
			connectNatsC := nats.NewTestContainer(t)
			natsES, err := nats.NewEventStore(nats.EventStoreConfig{
				Log:            slog.Default(),
				Connect:        connectNatsC,
				StreamSubjects: streamSubjects,
				SubjectPrefix:  subjectPrefix,
			})
			require.NoError(t, err)
			require.NotNil(t, natsES)

			natsSnapshotter, err := nats.NewSnapshotter(nats.KvConfig{
				Connect: connectNatsC,
				Bucket:  "foobar",
			})
			require.NoError(t, err)
			require.NotNil(t, natsSnapshotter)

			return testCase{
				name:        "3. store=nats, snapshotter=memory",
				store:       natsES,
				snapshotter: es.NewInMemorySnapshotter(),
			}
		}(),
	}
}

type Tef func(opts ...es.EnvOption) *es.TestingEnv
type TestFunc func(t *testing.T, tef Tef)

func eachStore(testFunc TestFunc) func(t *testing.T) {
	return func(t *testing.T) {

		for _, sut := range getStoreSUTs(t) {
			sut := sut
			t.Run(sut.name, func(t *testing.T) {
				testFunc(
					t,
					func(opts ...es.EnvOption) *es.TestingEnv {
						return es.StartTestEnv(
							t,
							es.WithSnapshotter(sut.snapshotter),
							es.WithStore(sut.store),
							es.WithAggregates(new(domain.TestAgg)),
							es.WithEnvOpts(opts...),
						)
					},
				)
			})
		}
	}
}

func TestEventStore_All(t *testing.T) {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	t.Run("start sequence", eachStore(func(t *testing.T, tef Tef) {
		sut := tef().Store()

		ar, err := sut.Append(
			t.Context(),
			"aggType",
			"aggID",
			0,
			[]es.Envelope{
				{ID: gonanoid.Must(), AggregateType: "test", AggregateID: "1234", Type: "test", Data: []byte(nil), OccurredAt: time.Now()},
				{ID: gonanoid.Must(), AggregateType: "test", AggregateID: "1235", Type: "test", Data: []byte(nil), OccurredAt: time.Now()},
			},
		)
		// TODO: protect against id+type missmatch
		require.NoError(t, err)
		require.Equal(t, uint64(2), ar.LastSeq)
	}))

	t.Run("create, mutate, load", eachStore(func(t *testing.T, tef Tef) {
		te := tef()

		a := domain.NewTestAgg("1000")
		require.Equal(t, "1000", a.GetID())
		require.Equal(t, 0, a.Count())

		t.Run("mutate", func(t *testing.T) {
			require.NoError(t, a.Inc())
			require.Equal(t, 1, a.Count())
			require.NoError(t, te.Repository().Save(t.Context(), a, es.WithSnapshot(true)))
		})

		t.Run("load", func(t *testing.T) {
			loaded := domain.NewTestAgg("1000")
			require.NoError(t, te.Repository().Load(t.Context(), loaded, es.WithSnapshot(true)))
			require.Equal(t, 1, loaded.Count())
			require.Equal(t, "1000", loaded.GetID())
			require.Equal(t, es.Version(1), loaded.GetVersion())
		})

		t.Run("inspect events", func(t *testing.T) {
			sut := te.Store()

			allEvents, err := sut.Load(t.Context(), a.GetAggType(), a.GetID())
			require.NoError(t, err)
			require.NotNil(t, allEvents)
			require.Len(t, allEvents, 1)

			first := allEvents[0]
			require.NotEmpty(t, first.Seq)
			require.Equal(t, es.Version(1), first.Version)
		})
	}))

	t.Run("snapshots", eachStore(func(t *testing.T, tef Tef) {
		var (
			te      = tef()
			tr      = es.NewTypedRepositoryFrom[*domain.TestAgg](slog.Default(), te.Repository())
			aggID   = "my-agg-" + gonanoid.Must()
			aggType = tr.GetAggType()
		)

		t.Run("get or create", func(t *testing.T) {
			a, err := tr.GetOrCreate(t.Context(), aggID, es.WithSnapshot(true))
			require.NoError(t, err)
			require.NotNil(t, a)
			require.Equal(t, aggID, a.GetID())
			require.Equal(t, es.Version(1), a.GetVersion())
		})

		t.Run("save agg with snapshot", func(t *testing.T) {
			a, err := tr.GetByID(t.Context(), aggID)
			require.NoError(t, err)
			require.NoError(t, a.Inc())
			require.NoError(t, tr.Save(t.Context(), a, es.WithSnapshot(true)))
			require.Equal(t, es.Version(2), a.GetVersion())
		})

		t.Run("load snapshot", func(t *testing.T) {
			ss, err := es.LoadSnapshot(t.Context(), te.Snapshotter(), aggType, aggID)
			require.NoError(t, err)
			require.NotNil(t, ss)
			require.NotEmpty(t, ss.SnapshotID)
			require.Equal(t, uint64(2), ss.StreamSeq)
			require.Equal(t, aggID, ss.ObjID)
			require.Equal(t, aggType, ss.ObjType)
			require.Equal(t, es.Version(2), ss.ObjVersion)
		})

		t.Run("apply snapshot", func(t *testing.T) {
			a := tr.NewWithID(aggID)
			require.NoError(t, es.ApplySnapshot(t.Context(), te.Snapshotter(), a))
			require.Equal(t, es.Version(2), a.GetVersion(), "version must be correct")
			require.Equal(t, uint64(2), a.GetSeq(), "seq must be correct")
			require.Equal(t, 1, a.Count(), "count must be correct")
		})

		t.Run("load with snapshot option", func(t *testing.T) {
			a, err := tr.GetByID(t.Context(), aggID, es.WithSnapshot(true))
			require.NoError(t, err)
			require.NotNil(t, a)
			require.EqualValues(t, 2, a.GetVersion(), "version must be correct")
			require.Equal(t, 1, a.Count(), "count must be correct")
		})
	}))

	t.Run("loadtest", eachStore(func(t *testing.T, tef Tef) {
		// --- config ---
		var (
			N     = 5_000
			aggID = "lt-5000"
			te    = tef()
		)

		// state
		a1 := domain.NewTestAgg(aggID)
		numMutations := 0
		numIncrements := 0

		for i := 0; i < N; i++ {
			require.NoError(t, a1.Inc())
			numMutations++
			numIncrements++

			require.Equal(t, numIncrements, a1.NumIncrements)

			// reset at 20
			if a1.Counter == 20 {
				require.NoError(t, a1.Reset())
				require.NoError(t, te.Repository().Save(t.Context(), a1))
				numMutations++
			}

			if i%1000 == 0 && i > 0 || i == N-20 {
				require.NoError(t, te.Repository().Save(t.Context(), a1, es.WithSnapshot(true)))
				require.Equal(t, es.Version(numMutations), a1.GetVersion())
			} else if i%100 == 0 && i > 0 {
				require.NoError(t, te.Repository().Save(t.Context(), a1))
				require.Equal(t, es.Version(numMutations), a1.GetVersion())
			}
		}

		// final save
		println("--- before save ---")
		require.NoError(t, te.Repository().Save(t.Context(), a1))
		require.Equal(t, es.Version(numMutations), a1.GetVersion())
		require.Equal(t, numIncrements, a1.NumIncrements)
		require.Equal(t, numIncrements, N)
		// do create another snapshot
		/*ss, err := te.Repository().CreateSnapshot(t.Context(), a1)
		require.NoError(t, err)
		require.NotNil(t, ss)*/
		println("=== SAVED ===")

		// === load ===
		println("=== LOADING ===")
		loadAt := time.Now()

		a2 := domain.NewTestAgg(aggID)
		require.NoError(t, te.Repository().Load(t.Context(), a2, es.WithSnapshot(true)))

		loadTook := time.Since(loadAt)
		t.Logf("load took: %s", loadTook)

		require.Equal(t, N, a2.NumIncrements)
		require.Equal(t, a1.Count(), a2.Count())
		require.Equal(t, a1.GetSeq(), a2.GetSeq())
	}))
}
