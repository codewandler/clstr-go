package nats

import (
	"fmt"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
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
}
