package cluster

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func CreateInMemoryTransport(t *testing.T) Transport {
	tr := NewInMemoryTransport()
	t.Cleanup(func() {
		require.NoError(t, tr.Close())
	})
	return tr
}

func CreateTestCluster(
	t *testing.T,
	tr ServerTransport,
	numNodes int,
	numShards uint32,
	shardSeed string,
	h ServerHandlerFunc,
) {
	nodeIDs := make([]string, 0)
	for i := 0; i < numNodes; i++ {
		nodeIDs = append(nodeIDs, fmt.Sprintf("node-%d", i))
	}

	for _, nodeID := range nodeIDs {
		n := NewNode(NodeOptions{
			NodeID:    nodeID,
			Transport: tr,
			Shards: ShardsForNode(
				nodeID,
				nodeIDs,
				numShards,
				shardSeed,
			),
			Handler: h,
		})

		require.NoError(t, n.Run(t.Context()))
	}
}
