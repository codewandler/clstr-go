package nats

import (
	"fmt"
	"log/slog"
	"testing"
	"time"

	gonanoid "github.com/matoous/go-nanoid/v2"
	"github.com/stretchr/testify/require"

	"github.com/codewandler/clstr-go/core/es"
)

func TestNats_Eventsourcing(t *testing.T) {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	connectNatsC := NewTestContainer(t)
	store, err := NewEventStore(EventStoreConfig{
		Connect: connectNatsC,
		Log:     slog.Default(),
	})
	require.NoError(t, err)
	require.NotNil(t, store)

	t.Run("stream info", func(t *testing.T) {
		si, err := store.stream.Info(t.Context())
		require.NoError(t, err)
		require.NotNil(t, si)
		require.Equal(t, "CLSTR_ES", si.Config.Name)
		require.Equal(t, uint64(1), si.Config.FirstSeq)
		require.Equal(t, []string{fmt.Sprintf("%s.>", defaultSubjectPrefix)}, si.Config.Subjects)
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
