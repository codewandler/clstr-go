package cluster

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNode_Lifecycle(t *testing.T) {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	var (
		tr      = NewInMemoryTransport().WithLog(slog.Default())
		handler = func(ctx context.Context, envelope Envelope) ([]byte, error) {
			return envelope.Data, nil
		}
	)

	// run node
	nodeCtx, nodeCtxCancel := context.WithCancel(t.Context())
	defer nodeCtxCancel()
	n := NewNode(NodeOptions{Transport: tr, Handler: handler, Shards: ShardsForNode("node-1", []string{"node-1"}, 16, "")})
	require.NoError(t, n.Run(nodeCtx))

	// we can publish a message
	_, err := tr.Request(t.Context(), Envelope{Shard: 0, Data: []byte("ping")})
	require.NoError(t, err)

	// when node no longer running, publish MUST fails
	nodeCtxCancel()
	<-time.After(50 * time.Millisecond)

	_, err = tr.Request(t.Context(), Envelope{Shard: 0, Data: []byte("ping")})
	require.Error(t, err)
}

func TestNode(t *testing.T) {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	var (
		tr      = NewInMemoryTransport()
		handler = func(ctx context.Context, envelope Envelope) ([]byte, error) {
			return envelope.Data, nil
		}
	)

	var (
		numShards = uint32(256)
	)

	CreateTestCluster(t, tr, 3, numShards, "pow", handler)

	res, err := tr.Request(t.Context(), Envelope{Shard: 2, Data: []byte("ping")})
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, []byte("ping"), res)

	res, err = tr.Request(t.Context(), Envelope{Shard: 3, Type: MsgClusterNodeInfo})
	require.NoError(t, err)
	require.NotNil(t, res)
}

func TestNode_Scoped(t *testing.T) {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	tr := CreateInMemoryTransport(t)

	CreateTestCluster(t, tr, 1, 16, "pow", NewKeyHandler(func(key string) (ServerHandlerFunc, error) {
		return func(ctx context.Context, env Envelope) ([]byte, error) {
			return []byte(key), nil
		}, nil
	}))

	c, err := NewClient(ClientOptions{
		NumShards: 16,
		Transport: tr,
		Seed:      "pow",
	})
	require.NoError(t, err)

	c1 := c.Key("tenant-1")
	data, err := c1.Request(t.Context(), "something")
	require.NoError(t, err)
	require.Equal(t, "tenant-1", string(data))

	c2 := c.Key("tenant-2")
	data, err = c2.Request(t.Context(), "something")
	require.NoError(t, err)
	require.Equal(t, "tenant-2", string(data))
}

func TestNode_handler_err(t *testing.T) {
	type msg struct{ V int }

	tr := NewInMemoryTransport()

	n := NewNode(NodeOptions{
		Transport: tr,
		NodeID:    "node-1",
		Shards:    ShardsForNode("node-1", []string{"node-1"}, 1, ""),
		Handler: func(ctx context.Context, env Envelope) ([]byte, error) {
			return nil, fmt.Errorf("boom")
		},
	})
	require.NoError(t, n.Run(t.Context()))

	// TODO: n.NewClient()

	c, err := NewClient(ClientOptions{
		NumShards: 1,
		Transport: tr,
	})
	require.NoError(t, err)
	require.NotNil(t, c)

	t.Run("request", func(t *testing.T) {
		res, err := c.Key("tenant-1").Request(t.Context(), msg{V: 1})
		require.ErrorContains(t, err, "boom")
		require.Nil(t, res)
	})

	t.Run("notify", func(t *testing.T) {
		err := c.Key("tenant-1").Notify(t.Context(), msg{V: 1})
		require.ErrorContains(t, err, "boom")
	})
}
