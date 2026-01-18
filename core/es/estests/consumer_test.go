package estests

import (
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/codewandler/clstr-go/core/es"
	"github.com/codewandler/clstr-go/core/es/estests/domain"
)

func TestConsumer(t *testing.T) {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	rcv := make(chan es.MsgCtx, 1)

	te := es.NewTestEnv(t, es.WithAggregates(new(domain.TestAgg)))
	c := te.NewConsumer(
		es.Handle(func(m es.MsgCtx) error {
			rcv <- m
			return nil
		}),
		es.WithConsumerName("banana"),
		es.WithLog(slog.Default()),
		es.WithMiddlewares(
			es.NewLogMiddleware(),
		),
	)
	require.NoError(t, c.Start(t.Context()))

	te.Assert().Append(t.Context(), es.Version(0), "test", "1234", domain.Incremented{Inc: 10})

	select {
	case <-rcv:
	case <-time.After(1 * time.Second):
		t.Fatal("timeout")
	}

	c.Stop()
}

func TestConsumer_WithCheckpoint(t *testing.T) {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	rcv := make(chan es.MsgCtx, 1)

	te := es.NewTestEnv(t, es.WithAggregates(new(domain.TestAgg)))
	cp := es.NewInMemCpStore()
	_ = cp.Set(5)

	c := te.NewConsumer(
		es.Handle(func(m es.MsgCtx) error {
			rcv <- m
			return nil
		}),
		es.WithConsumerName("banana"),
		es.WithLog(slog.Default()),
		es.WithMiddlewares(
			es.NewCheckpointMiddleware(cp),
			es.NewLogMiddleware(),
		),
	)
	require.NoError(t, c.Start(t.Context()))

	te.Assert().Append(t.Context(), es.Version(0), "test", "1234", domain.Incremented{Inc: 10})

	select {
	case <-rcv:
		t.Fatal("received event that should have been skipped by checkpoint")
	case <-time.After(100 * time.Millisecond):
	}

	c.Stop()
}

func TestConsumer_WithConsumer(t *testing.T) {
	done := make(chan struct{})
	te := es.NewTestEnv(
		t,
		es.WithAggregates(new(domain.TestAgg)),
		es.WithConsumer(
			es.Handle(func(ctx es.MsgCtx) error {
				close(done)
				return nil
			}),
		),
	)
	te.Assert().Append(t.Context(), es.Version(0), "test", "1234", domain.Incremented{Inc: 10})
	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("timeout")
	}
}
