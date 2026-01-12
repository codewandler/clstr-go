package integration

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/codewandler/clstr-go/core/actor/v2"
	"github.com/codewandler/clstr-go/core/cluster"
	"github.com/stretchr/testify/require"
)

type (
	myRequest struct {
		A int
		B int
	}
	myResponse     struct{ V int }
	myNotification struct{}
	myError        struct{}
)

func createActor(ctx context.Context, log *slog.Logger, tenantID string) actor.Actor {
	h := actor.TypedHandlers(
		actor.HandleMsg[myNotification](func(h actor.HandlerCtx, i myNotification) error {
			return nil
		}),
		actor.HandleMsg[myError](func(h actor.HandlerCtx, i myError) error {
			return fmt.Errorf("I failed")
		}),
		actor.HandleRequest[myRequest, myResponse](func(ctx actor.HandlerCtx, request myRequest) (*myResponse, error) {
			return &myResponse{V: request.A + request.B}, nil
		}),
	)

	return actor.New(actor.Options{
		Context: ctx,
		Logger:  log,
	}, h)
}

func TestIntegration(t *testing.T) {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	var (
		tr = cluster.CreateInMemoryTransport(t)
	)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	cluster.CreateTestCluster(
		t,
		tr,
		5,
		256,
		"foobar",
		cluster.NewActorHandler(func(key string) (actor.Actor, error) {
			return createActor(ctx, slog.Default(), key), nil
		}),
	)

	c, err := cluster.NewClient(cluster.ClientOptions{
		Seed:      "foobar",
		Transport: tr,
		NumShards: 256,
	})
	require.NoError(t, err)

	cc := c.Key("tenant-1")
	res, err := cluster.NewRequest[myRequest, myResponse](cc).Request(ctx, myRequest{A: 1, B: 2})
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, myResponse{V: 3}, *res)

	cc = c.Key("tenant-2")
	res, err = cluster.NewRequest[myRequest, myResponse](cc).Request(ctx, myRequest{A: 1, B: 2})
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, myResponse{V: 3}, *res)

	// publish
	cc = c.Key("tenant-3")
	require.NoError(t, cc.Notify(ctx, myNotification{}))
	<-time.After(100 * time.Millisecond)

	// error in handler
	cc = c.Key("tenant-4")
	require.ErrorContains(t, cc.Notify(ctx, myError{}), "I failed")
	<-time.After(100 * time.Millisecond)
}
