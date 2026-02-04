package cluster

// MsgClusterNodeInfo is the message type for querying node metadata.
const (
	MsgClusterNodeInfo = "clstr.node.info"
)

type (
	// GetNodeInfoRequest is a request for node metadata.
	// Send via [ScopedClient.GetNodeInfo].
	GetNodeInfoRequest struct{}

	// GetNodeInfoResponse contains node metadata returned by [GetNodeInfoRequest].
	GetNodeInfoResponse struct {
		// NodeID is the unique identifier of the responding node.
		NodeID string `json:"node_id"`
		// Shards is the list of shard IDs owned by this node.
		Shards []uint32 `json:"shards"`
	}
)

func (GetNodeInfoRequest) messageType() string { return MsgClusterNodeInfo }
