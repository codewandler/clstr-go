package cluster

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestClient(t *testing.T) {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	var (
		tr = CreateInMemoryTransport(t)
		h  = func(ctx context.Context, envelope Envelope) ([]byte, error) {
			t.Logf("received: %+v", envelope)
			return envelope.Data, nil
		}
		numShards = uint32(32)
	)

	CreateTestCluster(t, tr, 5, numShards, "test", h)

	c, err := NewClient(ClientOptions{
		Transport: tr,
		NumShards: numShards,
		Seed:      "test",
	})
	require.NoError(t, err)

	// scoped
	tc := c.Key("tenant-1")
	info, err := tc.GetNodeInfo(t.Context())
	require.NoError(t, err)
	require.NotNil(t, info)
	t.Logf("node info: %+v", info)

	require.NoError(t, tc.Notify(t.Context(), map[string]any{"goo": 23}))

	<-time.After(10 * time.Millisecond)

}
