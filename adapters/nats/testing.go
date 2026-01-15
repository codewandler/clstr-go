package nats

import (
	"context"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type Testing interface {
	require.TestingT
	Context() context.Context
	Logf(format string, args ...any)
	Cleanup(func())
}

func NewTestContainer(t Testing) Connector {
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
	require.NoError(t, err)

	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(natsC); err != nil {
			t.Errorf("failed to terminate container: %s", err.Error())
		}
	})

	ip, err := natsC.ContainerIP(t.Context())
	require.NoError(t, err)
	t.Logf("nats ip: %s", ip)
	return ConnectURL("nats://" + ip + ":4222")
}
