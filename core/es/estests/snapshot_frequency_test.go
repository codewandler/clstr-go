package estests

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/codewandler/clstr-go/core/es"
	"github.com/codewandler/clstr-go/core/es/estests/domain"
)

// countingSnapshotter wraps a Snapshotter and counts SaveSnapshot calls, so a
// test can assert how often snapshots are actually written.
type countingSnapshotter struct {
	inner es.Snapshotter
	saves atomic.Int64
}

func (c *countingSnapshotter) SaveSnapshot(ctx context.Context, s es.Snapshot, o es.SnapshotSaveOpts) error {
	c.saves.Add(1)
	return c.inner.SaveSnapshot(ctx, s, o)
}

func (c *countingSnapshotter) LoadSnapshot(ctx context.Context, objType, objID string) (es.Snapshot, error) {
	return c.inner.LoadSnapshot(ctx, objType, objID)
}

// incN drives `events` increments through WithTransaction with the given
// snapshot-every value and returns how many snapshots were actually written
// (post-create) and the resulting aggregate state.
func incN(t *testing.T, aggID string, events int, every uint64) (snapshotWrites int64, numIncrements int) {
	t.Helper()
	spy := &countingSnapshotter{inner: es.NewInMemorySnapshotter()}
	te := es.StartTestEnv(t, es.WithAggregates(new(domain.TestAgg)), es.WithSnapshotter(spy))
	r := es.NewTypedRepositoryFrom[*domain.TestAgg](slog.Default(), te.Repository())

	for i := 0; i < events; i++ {
		require.NoError(t, r.WithTransaction(t.Context(), aggID, func(a *domain.TestAgg) error {
			return a.Inc()
		}, es.WithSnapshotEvery(every), es.WithCreate()))
	}
	loaded, err := r.GetByID(t.Context(), aggID)
	require.NoError(t, err)
	return spy.saves.Load(), loaded.NumIncrements
}

// TestRepository_WithSnapshotEvery proves frequency-gated snapshots: every-N
// writes far fewer snapshots than every-event, and 0 writes none (beyond the
// create baseline) — while state stays correct via event replay in all cases.
func TestRepository_WithSnapshotEvery(t *testing.T) {
	const events = 16

	everyEvent, n1 := incN(t, "agg-every-1", events, 1)
	everyEight, n8 := incN(t, "agg-every-8", events, 8)
	never, n0 := incN(t, "agg-never", events, 0)

	// State is correct regardless of snapshot cadence.
	require.Equal(t, events, n1)
	require.Equal(t, events, n8)
	require.Equal(t, events, n0)

	// every-1 snapshots roughly per event; every-8 is several times fewer;
	// 0 never snapshots beyond the one-time create baseline.
	require.GreaterOrEqual(t, everyEvent, int64(events), "every-1 should snapshot ~per event")
	require.Less(t, everyEight, everyEvent, "every-8 must snapshot fewer times than every-1")
	require.LessOrEqual(t, everyEight, int64(4), "every-8 over 16 events should snapshot only a few times")
	require.LessOrEqual(t, never, int64(2), "snapshot-every-0 must not snapshot beyond the create baseline")
}
