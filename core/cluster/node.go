package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	gonanoid "github.com/matoous/go-nanoid/v2"
)

type (
	NodeOptions struct {
		Log       *slog.Logger
		NodeID    string
		Transport ServerTransport
		Shards    []uint32
		Handler   ServerHandlerFunc
	}

	Node struct {
		log    *slog.Logger
		nodeID string
		t      ServerTransport
		h      ServerHandlerFunc
		shards []uint32
	}
)

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

	return &Node{
		log:    log,
		nodeID: nodeID,
		t:      opts.Transport,
		shards: opts.Shards,
		h:      hdl,
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

	// use handler
	data, err = n.h(ctx, env)
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

func (n *Node) Run(ctx context.Context) error {
	n.log.Info("starting node", slog.Int("num_shards", len(n.shards)))
	for _, s := range n.shards {
		_, err := n.t.SubscribeShard(ctx, s, n.handleMsg)
		if err != nil {
			return fmt.Errorf("failed to subscribe to shard %d: %w", s, err)
		}
	}
	return nil
}
