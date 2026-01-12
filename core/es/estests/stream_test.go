package estests

import (
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/codewandler/clstr-go/core/es"
	"github.com/codewandler/clstr-go/core/es/estests/domain"
	"github.com/stretchr/testify/require"
)

func TestStream(t *testing.T) {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	et := es.NewTestEnv(
		t,
		es.WithInMemory(),
		es.WithSnapshotter(es.NewInMemorySnapshotter(slog.Default())),
		es.WithAggregates(new(domain.TestAgg)),
	)

	// === test ===

	s1, err := et.Store().Subscribe(t.Context())
	require.NoError(t, err)
	defer s1.Cancel()

	// change
	a := domain.NewTestAgg("a-1")
	go func() {
		for e := range s1.Chan() {
			t.Logf("#1: got event: %+v", e)
		}
	}()

	require.NoError(t, errors.Join(
		a.Inc(),
		a.Inc(),
		a.Inc(),
		et.Repository().Save(t.Context(), a),

		a.Inc(),
		a.Inc(),
		a.Inc(),
		et.Repository().Save(t.Context(), a),
	))

	// subscribe and get ALL
	s2, err := et.Store().Subscribe(t.Context(), es.WithDeliverPolicy(es.DeliverAllPolicy))
	require.NoError(t, err)
	defer s2.Cancel()

	for {
		select {
		case ev := <-s2.Chan():
			t.Logf("#2: got event: %+v", ev)
			require.Equal(t, "a-1", ev.AggregateID)
		case <-time.After(100 * time.Millisecond):
			return
		}
	}

}
