package cluster

const (
	MsgClusterNodeInfo = "clstr.node.info"
)

type (
	GetNodeInfoRequest  struct{}
	GetNodeInfoResponse struct {
		NodeID string   `json:"node_id"`
		Shards []uint32 `json:"shards"`
	}
)

func (GetNodeInfoRequest) messageType() string { return MsgClusterNodeInfo }
