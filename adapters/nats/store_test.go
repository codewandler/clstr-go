package nats

import (
	"log/slog"
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
}
