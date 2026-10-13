package estests

import (
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	gonanoid "github.com/matoous/go-nanoid/v2"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"

	"github.com/codewandler/clstr-go/adapters/nats"
	"github.com/codewandler/clstr-go/core/es"
	"github.com/codewandler/clstr-go/core/es/estests/domain"
)

// These tests reproduce what stream retention limits (MaxAge, MaxBytes,
// MaxMsgs) do to an aggregate whose snapshot lives outside the stream: the
// stream drops its oldest events, the snapshot stays. The states are built by
// purging the stream below a sequence, which is exactly what a limit-based
// trim does, and once through a real MaxMsgs limit.

const (
	trimStreamName    = "TRIM_TEST"
	trimSubjectPrefix = "trim.t"
)

type trimEnv struct {
	t      *testing.T
	store  *nats.EventStore
	stream jetstream.Stream
	repo   es.TypedRepository[*domain.TestAgg]
}

func newTrimEnv(t *testing.T, cfg nats.EventStoreConfig) *trimEnv {
	t.Helper()

	connect := nats.NewTestContainer(t)
	cfg.Connect = connect
	cfg.Log = slog.Default()
	cfg.StreamName = trimStreamName
	cfg.SubjectPrefix = trimSubjectPrefix
	cfg.StreamSubjects = []string{"trim.>"}
	if cfg.MaxAge == 0 && cfg.MaxBytes == 0 && cfg.MaxMsgs == 0 {
		cfg.MaxMsgs = 100_000
	}
	store, err := nats.NewEventStore(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	nc, err := connect()
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	stream, err := js.Stream(t.Context(), trimStreamName)
	require.NoError(t, err)

	te := es.StartTestEnv(t,
		es.WithStore(store),
		es.WithSnapshotter(es.NewInMemorySnapshotter()),
		es.WithAggregates(new(domain.TestAgg)),
	)

	return &trimEnv{
		t:      t,
		store:  store,
		stream: stream,
		repo:   es.NewTypedRepositoryFrom[*domain.TestAgg](slog.Default(), te.Repository()),
	}
}

// create writes the creation event (one message). Create also snapshots it.
func (e *trimEnv) create(id string) {
	e.t.Helper()
	_, err := e.repo.Create(e.t.Context(), id)
	require.NoError(e.t, err)
}

// inc appends n increments in one save (n messages), from a full replay.
func (e *trimEnv) inc(id string, n int, snapshot bool) {
	e.t.Helper()
	a, err := e.repo.GetByID(e.t.Context(), id)
	require.NoError(e.t, err)
	for range n {
		require.NoError(e.t, a.Inc())
	}
	// WithSnapshot(false) would still snapshot: the option ignores its value.
	var opts []es.SaveOption
	if snapshot {
		opts = append(opts, es.WithSnapshot(true))
	}
	require.NoError(e.t, e.repo.Save(e.t.Context(), a, opts...))
}

// trimBelow drops every message with a stream sequence below seq, as a
// retention limit does.
func (e *trimEnv) trimBelow(seq uint64) {
	e.t.Helper()
	require.NoError(e.t, e.stream.Purge(e.t.Context(), jetstream.WithPurgeSequence(seq)))
	require.Equal(e.t, seq, e.firstSeq())
}

func (e *trimEnv) firstSeq() uint64 {
	e.t.Helper()
	info, err := e.stream.Info(e.t.Context())
	require.NoError(e.t, err)
	return info.State.FirstSeq
}

func (e *trimEnv) loadFromSnapshot(id string) (*domain.TestAgg, error) {
	return e.repo.GetByID(e.t.Context(), id, es.WithSnapshot(true))
}

// snapshotOnly builds the snapshot-only state for id: snapshot at version 4,
// stream sequence 4; every event of the aggregate trimmed; the stream's first
// sequence is 5.
func (e *trimEnv) snapshotOnly(id string) {
	e.t.Helper()
	e.create(id)             // v1, seq 1
	e.inc(id, 3, true)       // v2..v4, seq 2..4, snapshot v4 at seq 4
	e.create("filler-" + id) // seq 5
	e.trimBelow(5)
}

func TestHistoryTrimmed_SnapshotOnly_SaveContinuesVersions(t *testing.T) {
	e := newTrimEnv(t, nats.EventStoreConfig{})
	e.snapshotOnly("a")

	a, err := e.loadFromSnapshot("a")
	require.NoError(t, err)
	require.Equal(t, es.Version(4), a.GetVersion())
	require.Equal(t, 3, a.Count())

	// The first save after the trim lands on an empty subject.
	require.NoError(t, a.Inc())
	require.NoError(t, e.repo.Save(t.Context(), a))
	require.Equal(t, es.Version(5), a.GetVersion())

	// The continuation is durable and follows the snapshot.
	a, err = e.loadFromSnapshot("a")
	require.NoError(t, err)
	require.Equal(t, es.Version(5), a.GetVersion())
	require.Equal(t, 4, a.Count())

	// The next save takes the ordinary path.
	require.NoError(t, a.Inc())
	require.NoError(t, e.repo.Save(t.Context(), a))
	a, err = e.loadFromSnapshot("a")
	require.NoError(t, err)
	require.Equal(t, es.Version(6), a.GetVersion())
	require.Equal(t, 5, a.Count())

	// A full replay cannot rebuild it: versions 1..4 are gone.
	_, err = e.repo.GetByID(t.Context(), "a")
	require.ErrorIs(t, err, es.ErrHistoryTrimmed)
}

func TestHistoryTrimmed_RealRetentionLimit(t *testing.T) {
	e := newTrimEnv(t, nats.EventStoreConfig{MaxMsgs: 8})

	e.create("a")       // seq 1
	e.inc("a", 3, true) // seq 2..4, snapshot v4 at seq 4
	for i := range 8 {  // seq 5..12; MaxMsgs drops seq 1..4
		e.create("filler-" + string(rune('a'+i)))
	}
	require.Equal(t, uint64(5), e.firstSeq())

	a, err := e.loadFromSnapshot("a")
	require.NoError(t, err)
	require.Equal(t, es.Version(4), a.GetVersion())
	require.NoError(t, a.Inc())
	require.NoError(t, e.repo.Save(t.Context(), a))

	a, err = e.loadFromSnapshot("a")
	require.NoError(t, err)
	require.Equal(t, es.Version(5), a.GetVersion())
	require.Equal(t, 4, a.Count())
}

func TestHistoryTrimmed_GapAfterSnapshot(t *testing.T) {
	e := newTrimEnv(t, nats.EventStoreConfig{})

	e.create("a")        // v1, seq 1
	e.inc("a", 1, true)  // v2, seq 2, snapshot v2 at seq 2
	e.inc("a", 2, false) // v3, v4 at seq 3, 4
	e.trimBelow(4)       // v3 (seq 3) is gone, v4 (seq 4) remains

	_, err := e.loadFromSnapshot("a")
	require.ErrorIs(t, err, es.ErrHistoryTrimmed)
	require.NotErrorIs(t, err, es.ErrVersionMismatch)
	require.ErrorContains(t, err, "expect version 3, got 4")
}

func TestHistoryTrimmed_NoSnapshotTrimmedStart(t *testing.T) {
	e := newTrimEnv(t, nats.EventStoreConfig{})

	e.create("a")        // v1, seq 1
	e.inc("a", 2, false) // v2, v3 at seq 2, 3
	e.trimBelow(3)

	_, err := e.repo.GetByID(t.Context(), "a")
	require.ErrorIs(t, err, es.ErrHistoryTrimmed)
	require.NotErrorIs(t, err, es.ErrVersionMismatch)
	require.ErrorContains(t, err, "expect version 1, got 3")
}

func TestHistoryTrimmed_ConcurrentWriterStillConflicts(t *testing.T) {
	e := newTrimEnv(t, nats.EventStoreConfig{})
	e.snapshotOnly("a")

	w1, err := e.loadFromSnapshot("a")
	require.NoError(t, err)
	w2, err := e.loadFromSnapshot("a")
	require.NoError(t, err)

	require.NoError(t, w1.Inc())
	require.NoError(t, e.repo.Save(t.Context(), w1))

	require.NoError(t, w2.IncBy(5))
	err = e.repo.Save(t.Context(), w2)
	require.ErrorIs(t, err, es.ErrConcurrencyConflict)

	a, err := e.loadFromSnapshot("a")
	require.NoError(t, err)
	require.Equal(t, es.Version(5), a.GetVersion())
	require.Equal(t, 4, a.Count(), "the first writer's event is the one stored")
}

// Both writers pass the empty-subject read before either publishes; the
// expected-last-subject-sequence 0 on the publish must still let only one in.
func TestHistoryTrimmed_RacingWritersOneWins(t *testing.T) {
	const n = 10
	e := newTrimEnv(t, nats.EventStoreConfig{})
	for i := range n {
		e.create(fmt.Sprintf("a%d", i)) // v1 at seq i+1
	}
	e.create("filler") // seq n+1
	e.trimBelow(n + 1)

	for i := range n {
		id := fmt.Sprintf("a%d", i)
		var (
			wg   sync.WaitGroup
			errs = make([]error, 2)
		)
		for w := range 2 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, errs[w] = e.store.Append(t.Context(), "test_agg", id, 1,
					[]es.Envelope{incEnvelope(id, 2)}, es.WithLastKnownSeq(uint64(i+1)))
			}()
		}
		wg.Wait()

		ok := 0
		for _, err := range errs {
			if err == nil {
				ok++
				continue
			}
			require.ErrorIs(t, err, es.ErrConcurrencyConflict)
		}
		require.Equal(t, 1, ok, "exactly one writer wins for %s", id)
	}
}

// An aggregate held in memory (a cache, a long-lived actor) whose stored
// events are trimmed before its next save continues like a snapshot-only one.
func TestHistoryTrimmed_HeldAggregateSavesAfterTrim(t *testing.T) {
	e := newTrimEnv(t, nats.EventStoreConfig{})
	e.create("a")        // v1, seq 1
	e.inc("a", 3, false) // v2..v4, seq 2..4, no snapshot after v1

	held, err := e.repo.GetByID(t.Context(), "a") // full replay: v4, seq 4
	require.NoError(t, err)
	e.create("filler") // seq 5
	e.trimBelow(5)

	require.NoError(t, held.Inc())
	require.NoError(t, e.repo.Save(t.Context(), held))
	require.Equal(t, es.Version(5), held.GetVersion())

	// The only snapshot is v1 at seq 1, so a later load from it hits the trim.
	_, err = e.loadFromSnapshot("a")
	require.ErrorIs(t, err, es.ErrHistoryTrimmed)
	require.ErrorContains(t, err, "expect version 2, got 5")
}

func TestHistoryTrimmed_EmptySubjectWithoutTrimStillConflicts(t *testing.T) {
	ctx := t.Context()

	t.Run("subject purged, stream not trimmed past the snapshot", func(t *testing.T) {
		e := newTrimEnv(t, nats.EventStoreConfig{})
		e.create("filler")  // seq 1 keeps the stream's first sequence at 1
		e.create("a")       // v1, seq 2
		e.inc("a", 3, true) // v2..v4, seq 3..5, snapshot v4 at seq 5
		require.NoError(t, e.stream.Purge(ctx, jetstream.WithPurgeSubject(trimSubjectPrefix+".test_agg.a")))
		require.Equal(t, uint64(1), e.firstSeq())

		a, err := e.loadFromSnapshot("a")
		require.NoError(t, err)
		require.Equal(t, es.Version(4), a.GetVersion())
		require.NoError(t, a.Inc())
		err = e.repo.Save(ctx, a)
		require.ErrorIs(t, err, es.ErrConcurrencyConflict)
		require.ErrorContains(t, err, "expected version 4, got 0")
	})

	t.Run("no last known sequence", func(t *testing.T) {
		e := newTrimEnv(t, nats.EventStoreConfig{})
		e.snapshotOnly("a")

		_, err := es.AppendEvents(ctx, e.store, "test_agg", "a", 4, &domain.Incremented{Inc: 1})
		require.ErrorIs(t, err, es.ErrConcurrencyConflict)
	})

	t.Run("last known sequence inside the retained range", func(t *testing.T) {
		e := newTrimEnv(t, nats.EventStoreConfig{})
		e.snapshotOnly("a")

		_, err := e.store.Append(ctx, "test_agg", "a", 4, []es.Envelope{
			incEnvelope("a", 5),
		}, es.WithLastKnownSeq(5))
		require.ErrorIs(t, err, es.ErrConcurrencyConflict)
	})
}

func TestHistoryTrimmed_NormalPathUnchanged(t *testing.T) {
	e := newTrimEnv(t, nats.EventStoreConfig{})

	e.create("a")        // v1, seq 1
	e.inc("a", 2, true)  // v2, v3, snapshot v3 at seq 3
	e.inc("a", 2, false) // v4, v5

	a, err := e.loadFromSnapshot("a")
	require.NoError(t, err)
	require.Equal(t, es.Version(5), a.GetVersion())
	require.Equal(t, 4, a.Count())
	require.NoError(t, a.Inc())
	require.NoError(t, e.repo.Save(t.Context(), a))

	a, err = e.repo.GetByID(t.Context(), "a")
	require.NoError(t, err)
	require.Equal(t, es.Version(6), a.GetVersion())
	require.Equal(t, 5, a.Count())
}

// A version gap the stream's trim does not explain is corruption, and must not
// be reported as trimmed history.
func TestVersionMismatch_NotTrimmed(t *testing.T) {
	for _, tc := range getStoreSUTs(t) {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			te := es.StartTestEnv(t,
				es.WithStore(tc.store),
				es.WithSnapshotter(tc.snapshotter),
				es.WithAggregates(new(domain.TestAgg)),
			)
			repo := es.NewTypedRepositoryFrom[*domain.TestAgg](slog.Default(), te.Repository())
			id := gonanoid.Must()

			_, err := repo.Create(ctx, id)
			require.NoError(t, err)
			// The store does not police envelope versions: write v3 after v1.
			_, err = tc.store.Append(ctx, "test_agg", id, 1, []es.Envelope{incEnvelope(id, 3)})
			require.NoError(t, err)

			_, err = repo.GetByID(ctx, id)
			require.ErrorIs(t, err, es.ErrVersionMismatch)
			require.NotErrorIs(t, err, es.ErrHistoryTrimmed)
			require.ErrorContains(t, err, "expect version 2, got 3")
		})
	}
}

func incEnvelope(aggID string, v es.Version) es.Envelope {
	return es.Envelope{
		ID:            gonanoid.Must(),
		Type:          "Incremented",
		AggregateID:   aggID,
		AggregateType: "test_agg",
		Version:       v,
		OccurredAt:    time.Now(),
		Data:          []byte(`{"inc":1}`),
	}
}
