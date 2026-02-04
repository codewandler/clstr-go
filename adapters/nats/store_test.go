package nats

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gonanoid "github.com/matoous/go-nanoid/v2"
	"github.com/stretchr/testify/require"

	"github.com/codewandler/clstr-go/core/es"
)

func newTestStore(t *testing.T) *EventStore {
	connectNatsC := NewTestContainer(t)
	store, err := NewEventStore(EventStoreConfig{
		Connect:       connectNatsC,
		Log:           slog.Default(),
		SubjectPrefix: "foo.tenant-1",
		StreamSubjects: []string{
			"foo.>",
		},
		MaxMsgs: 100_000, // require at least one retention limit
	})
	require.NoError(t, err)
	require.NotNil(t, store)
	return store
}

func TestStore_Append(t *testing.T) {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	store := newTestStore(t)

	require.Equal(t, "foo.tenant-1.test.1234", store.subjectForAggregate("test", "1234"))

	t.Run("stream info", func(t *testing.T) {
		si, err := store.stream.Info(t.Context())
		require.NoError(t, err)
		require.NotNil(t, si)
		require.Equal(t, "CLSTR_ES", si.Config.Name)
		require.Equal(t, uint64(1), si.Config.FirstSeq)
		require.Equal(t, []string{"foo.>"}, si.Config.Subjects)
	})

	t.Run("end state", func(t *testing.T) {
		cons := store.stream.ConsumerNames(t.Context())
		require.NoError(t, cons.Err())
		allNames := make([]string, 0)
		for n := range cons.Name() {
			allNames = append(allNames, n)
		}
		require.Equal(t, []string{}, allNames, "no dangling consumers")
	})

	t.Run("get last", func(t *testing.T) {
		res, err := store.Append(t.Context(), "test", "123", 0, []es.Envelope{
			{
				ID:            gonanoid.Must(),
				OccurredAt:    time.Now(),
				AggregateType: "test",
				AggregateID:   "123",
				Type:          "foobar",
				Version:       1,
			},
			{
				ID:            gonanoid.Must(),
				OccurredAt:    time.Now(),
				AggregateType: "test",
				AggregateID:   "123",
				Type:          "foobar",
				Version:       2,
			},
			{
				ID:            gonanoid.Must(),
				OccurredAt:    time.Now(),
				AggregateType: "test",
				AggregateID:   "123",
				Type:          "foobar",
				Version:       3,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, res)
		require.EqualValues(t, 3, res.LastSeq)

		v, err := store.getMostRecentEventForAgg(t.Context(), "test", "123")
		require.NoError(t, err)
		require.EqualValues(t, 3, v.Version)

		res, err = store.Append(t.Context(), "test", "123", 3, []es.Envelope{
			{
				ID:            gonanoid.Must(),
				OccurredAt:    time.Now(),
				AggregateType: "test",
				AggregateID:   "123",
				Type:          "foobar",
				Version:       4,
			},
			{
				ID:            gonanoid.Must(),
				OccurredAt:    time.Now(),
				AggregateType: "test",
				AggregateID:   "123",
				Type:          "foobar",
				Version:       5,
			},
			{
				ID:            gonanoid.Must(),
				OccurredAt:    time.Now(),
				AggregateType: "test",
				AggregateID:   "123",
				Type:          "foobar",
				Version:       6,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, res)
		require.EqualValues(t, 6, res.LastSeq)
	})
}

func TestStore_Latency(t *testing.T) {
	var (
		N     = 1_000
		M     = 200
		R     = 980
		aggID = "agg-123"
	)
	store := newTestStore(t)

	for i := 0; i < N; i++ {
		res, err := store.Append(t.Context(), "test", aggID, es.Version(i), []es.Envelope{
			{
				ID:            gonanoid.Must(),
				OccurredAt:    time.Now(),
				AggregateType: "test",
				AggregateID:   aggID,
				Type:          "foobar",
				Version:       es.Version(i + 1),
			},
		})
		require.NoError(t, err)
		require.NotNil(t, res)
	}

	startAt := time.Now()
	for i := 0; i < M; i++ {
		events, err := store.Load(t.Context(), "test", aggID, es.WithStartSeq(uint64(R+1)))
		require.NoError(t, err)
		require.Equal(t, len(events), N-R)
	}
	took := time.Since(startAt)
	perLoad := took / time.Duration(M)
	t.Logf("took %s, per_item: %s", took, perLoad)
}

func TestStore_Subscribe(t *testing.T) {
	t.Run("with start sequence", func(t *testing.T) {
		s := newTestStore(t)
		for i := 0; i < 3; i++ {
			_, err := es.AppendEvents(t.Context(), s, "test", "123", es.Version(i), []any{1, 2, 3})
			require.NoError(t, err)
		}

		sub, err := s.Subscribe(t.Context(), es.WithStartSequence(3), es.WithDeliverPolicy(es.DeliverAllPolicy))
		require.NoError(t, err)
		require.NotNil(t, sub)

		select {
		case <-time.After(time.Second):
			t.Fatal("timeout")
		case ev := <-sub.Chan():
			t.Logf("got event: %+v", ev)
			require.EqualValues(t, es.Version(3), ev.Version)
		}

		sub.Cancel()
	})

	t.Run("unsubscribe while writing", func(t *testing.T) {
		s := newTestStore(t)

		sub, err := s.Subscribe(t.Context(), es.WithStartSequence(3), es.WithDeliverPolicy(es.DeliverAllPolicy))
		require.NoError(t, err)
		require.NotNil(t, sub)

		go func() {
			for i := 0; i < 200; i++ {
				_, err := es.AppendEvents(t.Context(), s, "test", "123", es.Version(i), []any{1, 2, 3})
				if errors.Is(err, context.Canceled) {
					return
				}
				require.NoError(t, err)
				<-time.After(10 * time.Millisecond)
			}
		}()

		<-time.After(500 * time.Millisecond)
		sub.Cancel()
	})
}

func TestStore_RetentionRequired(t *testing.T) {
	connectNatsC := NewTestContainer(t)

	t.Run("fails without retention limit", func(t *testing.T) {
		_, err := NewEventStore(EventStoreConfig{
			Connect:       connectNatsC,
			Log:           slog.Default(),
			SubjectPrefix: "foo.tenant-1",
			StreamSubjects: []string{
				"foo.>",
			},
			// No MaxAge, MaxBytes, or MaxMsgs set
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "at least one retention limit must be set")
	})

	t.Run("succeeds with MaxAge", func(t *testing.T) {
		store, err := NewEventStore(EventStoreConfig{
			Connect:       connectNatsC,
			Log:           slog.Default(),
			SubjectPrefix: "foo.tenant-2",
			StreamSubjects: []string{
				"foo.>",
			},
			MaxAge: 24 * time.Hour,
		})
		require.NoError(t, err)
		require.NotNil(t, store)
		defer store.Close()
	})

	t.Run("succeeds with MaxBytes", func(t *testing.T) {
		store, err := NewEventStore(EventStoreConfig{
			Connect:       connectNatsC,
			Log:           slog.Default(),
			SubjectPrefix: "bytes.tenant",
			StreamName:    "TEST_BYTES",
			StreamSubjects: []string{
				"bytes.>",
			},
			MaxBytes: 100 * 1024 * 1024, // 100MB
		})
		require.NoError(t, err)
		require.NotNil(t, store)
		defer store.Close()
	})

	t.Run("succeeds with MaxMsgs", func(t *testing.T) {
		store, err := NewEventStore(EventStoreConfig{
			Connect:       connectNatsC,
			Log:           slog.Default(),
			SubjectPrefix: "msgs.tenant",
			StreamName:    "TEST_MSGS",
			StreamSubjects: []string{
				"msgs.>",
			},
			MaxMsgs: 10_000,
		})
		require.NoError(t, err)
		require.NotNil(t, store)
		defer store.Close()
	})

	t.Run("retention limits applied to stream", func(t *testing.T) {
		store, err := NewEventStore(EventStoreConfig{
			Connect:       connectNatsC,
			Log:           slog.Default(),
			StreamName:    "TEST_RETENTION",
			SubjectPrefix: "retention.test",
			StreamSubjects: []string{
				"retention.>",
			},
			MaxAge:   1 * time.Hour,
			MaxBytes: 1024 * 1024,
			MaxMsgs:  1000,
		})
		require.NoError(t, err)
		require.NotNil(t, store)
		defer store.Close()

		// Verify stream config
		si, err := store.stream.Info(t.Context())
		require.NoError(t, err)
		require.Equal(t, 1*time.Hour, si.Config.MaxAge)
		require.Equal(t, int64(1024*1024), si.Config.MaxBytes)
		require.Equal(t, int64(1000), si.Config.MaxMsgs)
	})
}

func TestStore_RenameType(t *testing.T) {
	connectNatsC := NewTestContainer(t)

	store, err := NewEventStore(EventStoreConfig{
		Connect:       connectNatsC,
		Log:           slog.Default(),
		SubjectPrefix: "rename.test",
		StreamSubjects: []string{
			"rename.>",
		},
		MaxMsgs: 10_000,
		RenameType: func(aggType string) string {
			// Simulate renaming from old to new aggregate type
			return strings.ReplaceAll(aggType, "old_", "new_")
		},
	})
	require.NoError(t, err)
	require.NotNil(t, store)
	defer store.Close()

	t.Run("subject uses renamed type", func(t *testing.T) {
		subject := store.subjectForAggregate("old_user", "123")
		require.Equal(t, "rename.test.new_user.123", subject)
	})

	t.Run("append and load with renamed type", func(t *testing.T) {
		res, err := store.Append(t.Context(), "old_user", "456", 0, []es.Envelope{
			{
				ID:            gonanoid.Must(),
				OccurredAt:    time.Now(),
				AggregateType: "old_user",
				AggregateID:   "456",
				Type:          "UserCreated",
				Version:       1,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, res)
		require.EqualValues(t, 1, res.LastSeq)

		// Load should work with the same type
		events, err := store.Load(t.Context(), "old_user", "456")
		require.NoError(t, err)
		require.Len(t, events, 1)
	})
}

func TestStore_ConcurrentAppend(t *testing.T) {
	store := newTestStore(t)

	const (
		numAggregates   = 10
		numWorkers      = 5
		eventsPerWorker = 20
	)

	var wg sync.WaitGroup
	errChan := make(chan error, numWorkers*numAggregates)
	var totalAppends atomic.Int64

	for range numAggregates {
		aggID := gonanoid.Must()

		for workerIdx := range numWorkers {
			wg.Add(1)
			go func(aggID string, workerIdx int) {
				defer wg.Done()

				for i := range eventsPerWorker {
					// Each worker tries to append - most will fail due to concurrency
					_, err := store.Append(t.Context(), "test", aggID, es.Version(i), []es.Envelope{
						{
							ID:            gonanoid.Must(),
							OccurredAt:    time.Now(),
							AggregateType: "test",
							AggregateID:   aggID,
							Type:          "TestEvent",
							Version:       es.Version(i + 1),
							Data:          []byte(`{"worker":` + string(rune('0'+workerIdx)) + `}`),
						},
					})
					if err == nil {
						totalAppends.Add(1)
					} else if !errors.Is(err, es.ErrConcurrencyConflict) {
						errChan <- err
					}
				}
			}(aggID, workerIdx)
		}
	}

	wg.Wait()
	close(errChan)

	// Check for unexpected errors
	for err := range errChan {
		require.NoError(t, err, "unexpected error during concurrent append")
	}

	// At least some appends should succeed (one per aggregate per version)
	require.Greater(t, totalAppends.Load(), int64(0), "some appends should succeed")
	t.Logf("total successful appends: %d", totalAppends.Load())
}

func TestStore_CleanupOnClose(t *testing.T) {
	connectNatsC := NewTestContainer(t)

	store, err := NewEventStore(EventStoreConfig{
		Connect:       connectNatsC,
		Log:           slog.Default(),
		SubjectPrefix: "cleanup.test",
		StreamSubjects: []string{
			"cleanup.>",
		},
		MaxMsgs: 10_000,
	})
	require.NoError(t, err)
	require.NotNil(t, store)

	// Append some events
	for i := range 5 {
		_, err := store.Append(t.Context(), "test", "agg-1", es.Version(i), []es.Envelope{
			{
				ID:            gonanoid.Must(),
				OccurredAt:    time.Now(),
				AggregateType: "test",
				AggregateID:   "agg-1",
				Type:          "TestEvent",
				Version:       es.Version(i + 1),
			},
		})
		require.NoError(t, err)
	}

	// Load events to create an ordered consumer
	events, err := store.Load(t.Context(), "test", "agg-1")
	require.NoError(t, err)
	require.Len(t, events, 5)

	// Close the store
	err = store.Close()
	require.NoError(t, err)

	// Connection should be drained/closed
	require.True(t, store.nc.IsClosed() || store.nc.IsDraining(), "connection should be closed or draining after Close()")
}

func TestStore_SubscribeCleanup(t *testing.T) {
	store := newTestStore(t)

	ctx, cancel := context.WithCancel(t.Context())

	// Create subscription
	sub, err := store.Subscribe(ctx, es.WithDeliverPolicy(es.DeliverNewPolicy))
	require.NoError(t, err)
	require.NotNil(t, sub)

	// Append some events
	for i := range 3 {
		_, err := store.Append(t.Context(), "test", "agg-sub", es.Version(i), []es.Envelope{
			{
				ID:            gonanoid.Must(),
				OccurredAt:    time.Now(),
				AggregateType: "test",
				AggregateID:   "agg-sub",
				Type:          "TestEvent",
				Version:       es.Version(i + 1),
			},
		})
		require.NoError(t, err)
	}

	// Receive some events
	select {
	case <-sub.Chan():
		// Got an event, good
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for event")
	}

	// Cancel context - this should trigger cleanup
	cancel()

	// Wait for channel to close (cleanup complete)
	select {
	case _, ok := <-sub.Chan():
		if !ok {
			// Channel closed, cleanup successful
		}
	case <-time.After(2 * time.Second):
		t.Fatal("subscription channel did not close after context cancellation")
	}
}
