package redis

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Testing is the subset of *testing.T required by test helpers in this package.
// It mirrors the interface used in adapters/nats.
type Testing interface {
	require.TestingT
	Context() context.Context
	Logf(format string, args ...any)
	Cleanup(func())
}

// NewTestClient spins up a redis:7-alpine testcontainer and returns a connected
// *redis.Client. The container is terminated automatically via t.Cleanup.
func NewTestClient(t Testing) *redis.Client {
	ctx := t.Context()

	redisC, err := testcontainers.Run(
		ctx, "redis:7-alpine",
		testcontainers.WithExposedPorts("6379/tcp"),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("6379/tcp"),
		),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(redisC); err != nil {
			t.Errorf("failed to terminate redis container: %s", err.Error())
		}
	})

	ip, err := redisC.ContainerIP(ctx)
	require.NoError(t, err)
	t.Logf("redis ip: %s", ip)

	client := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("%s:6379", ip),
	})

	// Verify connectivity.
	require.NoError(t, client.Ping(ctx).Err())

	t.Cleanup(func() { _ = client.Close() })

	return client
}
