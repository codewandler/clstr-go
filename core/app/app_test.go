package app

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/codewandler/clstr-go/core/actor/v2"
	"github.com/codewandler/clstr-go/core/cluster"
)

type (
	ping struct{ Seq int }
	pong struct{ Seq int }
)

func TestApp(t *testing.T) {
	slog.SetLogLoggerLevel(slog.LevelInfo)

	app, err := Run(
		Config{},
		actor.HandleRequest[ping, pong](func(h actor.HandlerCtx, i ping) (*pong, error) {
			return &pong{Seq: i.Seq + 1}, nil
		}),
	)
	require.NoError(t, err)
	require.NotNil(t, app)

	myClient := app.Client().Key("tenant-1")
	pr, err := cluster.NewRequest[ping, pong](myClient).Request(t.Context(), ping{Seq: 1})
	require.NoError(t, err)
	require.NotNil(t, pr)
	require.Equal(t, 2, pr.Seq)
}

func TestApp_Node(t *testing.T) {
	app, err := Run(Config{})
	require.NoError(t, err)
	require.NotNil(t, app.Node(), "Node() should be accessible")
}

func TestApp_ActorOptions(t *testing.T) {
	slog.SetLogLoggerLevel(slog.LevelInfo)

	app, err := Run(
		Config{
			Actor: ActorOptions{
				MailboxSize:        512,
				MaxConcurrentTasks: 10,
			},
		},
		actor.HandleRequest[ping, pong](func(h actor.HandlerCtx, i ping) (*pong, error) {
			return &pong{Seq: i.Seq + 1}, nil
		}),
	)
	require.NoError(t, err)
	require.NotNil(t, app)

	pr, err := cluster.NewRequest[ping, pong](app.Client().Key("key")).Request(t.Context(), ping{Seq: 5})
	require.NoError(t, err)
	require.Equal(t, 6, pr.Seq)
}

func TestApp_Shutdown(t *testing.T) {
	app, err := Run(Config{})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err = app.Shutdown(ctx)
	require.NoError(t, err)

	// Done channel should be closed
	select {
	case <-app.Done():
		// ok
	default:
		t.Fatal("Done() should be closed after Shutdown")
	}
}

func TestApp_Stop(t *testing.T) {
	app, err := Run(Config{})
	require.NoError(t, err)

	app.Stop()

	// Should be idempotent
	app.Stop()

	select {
	case <-app.Done():
		// ok
	case <-time.After(time.Second):
		t.Fatal("Done() should be closed after Stop")
	}
}

func TestApp_CustomNodeConfig(t *testing.T) {
	transport := cluster.NewInMemoryTransport()

	app, err := Run(Config{
		Node: NodeConfig{
			ID:        "my-node",
			NumShards: 64,
			ShardSeed: "custom-seed",
			Transport: transport,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, app)
}
