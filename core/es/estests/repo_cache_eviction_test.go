package estests

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/codewandler/clstr-go/core/es"
	"github.com/codewandler/clstr-go/core/es/estests/domain"
)

// flakyStore fails exactly one Append when armed, mimicking a transient
// event-store write failure (e.g. a NATS publish timeout).
type flakyStore struct {
	es.EventStore
	failNext atomic.Bool
}

var errTransientAppend = errors.New("transient append failure")

func (s *flakyStore) Append(ctx context.Context, aggType string, aggID string, expectedVersion es.Version, events []es.Envelope, opts ...es.StoreAppendOption) (*es.StoreAppendResult, error) {
	if s.failNext.CompareAndSwap(true, false) {
		return nil, errTransientAppend
	}
	return s.EventStore.Append(ctx, aggType, aggID, expectedVersion, events, opts...)
}

// A transaction mutates the LRU-cached aggregate in place before Save; a
// failed Save must therefore evict the aggregate from the cache, or the
// applied-but-unpersisted state is served on every later load — permanently.
// On acd this poisoned calls with a SelectedAgentID that was never written,
// producing an infinite re-match spin (DEV-726).
func TestWithTransaction_FailedSaveDoesNotPoisonCache(t *testing.T) {
	store := &flakyStore{EventStore: es.NewInMemoryStore()}
	env := es.NewEnv(
		es.WithSnapshotter(es.NewInMemorySnapshotter()),
		es.WithStore(store),
		es.WithAggregates(new(domain.TestAgg)),
	)
	require.NoError(t, env.Start())

	repo := es.NewTypedRepositoryFrom[*domain.TestAgg](slog.Default(), env.Repository(), es.WithRepoCacheLRU(100))
	const aggID = "agg-1"

	_, err := repo.Create(t.Context(), aggID)
	require.NoError(t, err)
	require.NoError(t, repo.WithTransaction(t.Context(), aggID, func(a *domain.TestAgg) error {
		return a.IncBy(1)
	}))

	// One transient write failure. The increment was applied to the cached
	// instance but never persisted.
	store.failNext.Store(true)
	err = repo.WithTransaction(t.Context(), aggID, func(a *domain.TestAgg) error {
		return a.IncBy(20)
	})
	require.ErrorIs(t, err, errTransientAppend)

	// The failed increment must not be visible: the next load has to reflect
	// the store, not the dirty cached instance.
	loaded, err := repo.GetByID(t.Context(), aggID)
	require.NoError(t, err)
	require.EqualValues(t, 1, loaded.Count(),
		"failed transaction leaked applied-but-unpersisted state out of the cache")

	// A retry must succeed and apply the mutation exactly once — no stale
	// uncommitted events from the failed attempt may tag along.
	require.NoError(t, repo.WithTransaction(t.Context(), aggID, func(a *domain.TestAgg) error {
		return a.IncBy(20)
	}))
	loaded, err = repo.GetByID(t.Context(), aggID)
	require.NoError(t, err)
	require.EqualValues(t, 21, loaded.Count())
}

// The same poisoning exists on the direct Save path: a failed Save leaves the
// caller's aggregate — often the cached instance — ahead of the store.
func TestSave_FailureEvictsCache(t *testing.T) {
	store := &flakyStore{EventStore: es.NewInMemoryStore()}
	env := es.NewEnv(
		es.WithSnapshotter(es.NewInMemorySnapshotter()),
		es.WithStore(store),
		es.WithAggregates(new(domain.TestAgg)),
	)
	require.NoError(t, env.Start())

	repo := es.NewTypedRepositoryFrom[*domain.TestAgg](slog.Default(), env.Repository(), es.WithRepoCacheLRU(100))
	const aggID = "agg-2"

	a, err := repo.Create(t.Context(), aggID)
	require.NoError(t, err)

	require.NoError(t, a.IncBy(1))
	store.failNext.Store(true)
	require.ErrorIs(t, repo.Save(t.Context(), a), errTransientAppend)

	loaded, err := repo.GetByID(t.Context(), aggID)
	require.NoError(t, err)
	require.EqualValues(t, 0, loaded.Count(),
		"failed Save left a dirty aggregate reachable through the cache")
}

// optsDroppingStore is a wrapper that does not pass the Append options on.
type optsDroppingStore struct{ es.EventStore }

func (s *optsDroppingStore) Append(ctx context.Context, aggType string, aggID string, expectedVersion es.Version, events []es.Envelope, _ ...es.StoreAppendOption) (*es.StoreAppendResult, error) {
	return s.EventStore.Append(ctx, aggType, aggID, expectedVersion, events)
}

// A cached aggregate whose stored events were trimmed saves through a store
// wrapper only if the wrapper passes the Append options on; one that drops
// them turns every such save into a conflict.
func TestWithTransaction_CachedAggregateAfterTrim(t *testing.T) {
	for _, tc := range []struct {
		name    string
		wrap    func(es.EventStore) es.EventStore
		wantErr error
	}{
		{"wrapper passes options on", func(s es.EventStore) es.EventStore { return &flakyStore{EventStore: s} }, nil},
		{"wrapper drops options", func(s es.EventStore) es.EventStore { return &optsDroppingStore{EventStore: s} }, es.ErrConcurrencyConflict},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inner := es.NewInMemoryStore()
			env := es.NewEnv(
				es.WithSnapshotter(es.NewInMemorySnapshotter()),
				es.WithStore(tc.wrap(inner)),
				es.WithAggregates(new(domain.TestAgg)),
			)
			require.NoError(t, env.Start())
			repo := es.NewTypedRepositoryFrom[*domain.TestAgg](slog.Default(), env.Repository(), es.WithRepoCacheLRU(100))

			_, err := repo.Create(t.Context(), "agg-3") // v1, seq 1, cached
			require.NoError(t, err)
			require.NoError(t, repo.WithTransaction(t.Context(), "agg-3", func(a *domain.TestAgg) error {
				return a.IncBy(1) // v2, seq 2
			}))
			_, err = repo.Create(t.Context(), "filler") // seq 3
			require.NoError(t, err)
			inner.TrimBefore(3)

			err = repo.WithTransaction(t.Context(), "agg-3", func(a *domain.TestAgg) error {
				return a.IncBy(1)
			})
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			loaded, err := repo.GetByID(t.Context(), "agg-3")
			require.NoError(t, err)
			require.Equal(t, es.Version(3), loaded.GetVersion())
			require.EqualValues(t, 2, loaded.Count())
		})
	}
}
