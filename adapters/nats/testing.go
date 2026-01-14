package nats

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func NewTestContainer(t *testing.T) Connector {
	ctx := t.Context()
	natsC, err := testcontainers.Run(
		ctx, "nats:latest",
		testcontainers.WithCmd("-js"),
		testcontainers.WithExposedPorts("4222/tcp"),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("4222/tcp"),
			wait.ForLog("Server is ready"),
		),
	)
	testcontainers.CleanupContainer(t, natsC)
	require.NoError(t, err)

	ip, err := natsC.ContainerIP(t.Context())
	require.NoError(t, err)
	t.Logf("nats ip: %s", ip)
	return ReuseConnection(ConnectURL("nats://" + ip + ":4222"))
}
