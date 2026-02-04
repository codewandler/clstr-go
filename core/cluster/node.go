package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	gonanoid "github.com/matoous/go-nanoid/v2"
)

type (
	// NodeOptions configures node creation.
	NodeOptions struct {
		// Log is the logger for node operations. Defaults to slog.Default().
		Log *slog.Logger
		// NodeID is the unique identifier for this node. Auto-generated if empty.
		NodeID string
		// Transport is the server transport for receiving messages (required).
		Transport ServerTransport
		// Shards is the list of shard IDs this node owns.
		// Use [ShardsForNode] to compute this based on cluster membership.
		Shards []uint32
		// Handler processes incoming messages for owned shards.
		Handler ServerHandlerFunc
		// Metrics for node instrumentation. If nil, a no-op implementation is used.
		Metrics ClusterMetrics
	}

	// Node represents a cluster member that owns and processes shards.
	// Create via [NewNode] and start with [Node.Run].
	Node struct {
		log     *slog.Logger
		nodeID  string
		t       ServerTransport
		h       ServerHandlerFunc
		shards  []uint32
		metrics ClusterMetrics
	}
)

// NewNode creates a new cluster node with the given options.
func NewNode(opts NodeOptions) *Node {
	log := opts.Log
	if log == nil {
		log = slog.Default()
	}

	nodeID := opts.NodeID
	if nodeID == "" {
		nodeID = fmt.Sprintf("node-%s", gonanoid.Must(6))
	}

	hdl := opts.Handler
	if hdl == nil {
		hdl = func(ctx context.Context, env Envelope) ([]byte, error) {
			return nil, fmt.Errorf("no handler registered")
		}
	}

	metrics := opts.Metrics
	if metrics == nil {
		metrics = NopClusterMetrics()
	}

	return &Node{
		log:     log,
		nodeID:  nodeID,
		t:       opts.Transport,
		shards:  opts.Shards,
		h:       hdl,
		metrics: metrics,
	}
}

func (n *Node) handleMsg(ctx context.Context, env Envelope) (data []byte, err error) {
	n.log.Debug(
		"handle",
		slog.Group(
			"envelope",
			slog.Int("shard", env.Shard),
			slog.String("type", env.Type),
			slog.Any("headers", env.Headers),
		),
	)

	// === handle internal messages ===

	switch env.Type {
	case MsgClusterNodeInfo:
		return json.Marshal(GetNodeInfoResponse{
			NodeID: n.nodeID,
			Shards: n.shards,
		})
	}

	// instrument handler execution
	defer n.metrics.HandlerDuration(env.Type).ObserveDuration()

	// use handler
	data, err = n.h(ctx, env)
	n.metrics.HandlerCompleted(env.Type, err == nil)
	if err != nil {
		n.log.Error(
			"failed to handle message",
			slog.Group(
				"message",
				slog.String("type", env.Type),
				slog.Any("headers", env.Headers),
				slog.String("data", string(env.Data)),
			),
			slog.Any("error", err),
		)
	}
	return
}

// Run starts the node, subscribing to all owned shards and handling incoming messages.
// Returns an error if subscription fails for any shard.
func (n *Node) Run(ctx context.Context) error {
	n.log.Info("starting node", slog.Int("num_shards", len(n.shards)))
	for _, s := range n.shards {
		_, err := n.t.SubscribeShard(ctx, s, n.handleMsg)
		if err != nil {
			return fmt.Errorf("failed to subscribe to shard %d: %w", s, err)
		}
	}

	// report shards owned
	n.metrics.ShardsOwned(n.nodeID, len(n.shards))

	return nil
}
