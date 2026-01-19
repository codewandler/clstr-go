package nats

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/codewandler/clstr-go/core/cluster"
)

func TestNats_Transport(t *testing.T) {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	connectNatsC := NewTestContainer(t)

	t.Run("connect & close", func(t *testing.T) {
		nc, err := connectNatsC()
		require.NoError(t, err)
		require.NotNil(t, nc)
		require.NoError(t, nc.Flush())
		require.NoError(t, nc.Drain())
		nc.Close()
	})

	t.Run("connectJetstream & close", func(t *testing.T) {
		tp, err := NewTransport(TransportConfig{
			Connect:       connectNatsC,
			Log:           slog.Default(),
			SubjectPrefix: "test",
		})
		require.NoError(t, err)
		require.NotNil(t, tp)

		s, err := tp.SubscribeShard(t.Context(), 23, func(ctx context.Context, env cluster.Envelope) ([]byte, error) {
			return env.Data, nil
		})
		require.NoError(t, err)
		require.NotNil(t, s)

		data, err := tp.Request(t.Context(), cluster.Envelope{Shard: 23, Data: []byte("hello")})
		require.NoError(t, err)
		require.NotNil(t, data)
		require.Equal(t, "hello", string(data))

		// tear down
		require.NoError(t, s.Unsubscribe())
		require.NoError(t, tp.Close())
	})

}
