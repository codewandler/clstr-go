// Package cluster provides distributed shard-based message routing with
// consistent hashing for building scalable, partitioned systems.
//
// The cluster package enables horizontal scaling by partitioning work across
// multiple nodes. Each node owns a subset of shards, and messages are routed
// to the appropriate node based on a key-to-shard mapping.
//
// # Architecture
//
// The cluster consists of three main components:
//
//   - [Client]: Routes messages to shards via key or shard ID
//   - [Node]: Subscribes to shards and handles incoming messages
//   - [Transport]: Abstracts the underlying messaging infrastructure
//
// # Shard Distribution
//
// Shards are distributed across nodes using Highest Random Weight (HRW)
// consistent hashing via [ShardsForNode]. This ensures:
//
//   - Even distribution of shards across nodes
//   - Minimal shard movement when nodes join or leave
//   - Deterministic assignment given the same node list
//
// Keys are mapped to shards using [ShardFromString], which provides
// consistent, uniform distribution using BLAKE2b hashing.
//
// # Client Usage
//
// Create a client to send messages to the cluster:
//
//	client, err := cluster.NewClient(cluster.ClientOptions{
//	    Transport: natsTransport,
//	    NumShards: 64,
//	})
//
//	// Route by key (e.g., user ID, tenant ID)
//	resp, err := client.Key("user:123").Request(ctx, GetUserQuery{ID: "123"})
//
//	// Route directly to shard
//	resp, err := client.Shard(5).Request(ctx, payload)
//
// # Node Usage
//
// Create a node to handle messages for owned shards:
//
//	node := cluster.NewNode(cluster.NodeOptions{
//	    NodeID:    "node-1",
//	    Transport: natsTransport,
//	    Shards:    cluster.ShardsForNode("node-1", allNodeIDs, 64, ""),
//	    Handler: func(ctx context.Context, env cluster.Envelope) ([]byte, error) {
//	        // Handle the message based on env.Type
//	        return response, nil
//	    },
//	})
//	node.Run(ctx)
//
// # Transport Layer
//
// The transport layer is abstracted via [Transport], [ClientTransport], and
// [ServerTransport] interfaces. The adapters/nats package provides a
// NATS JetStream implementation.
//
// # Envelope
//
// Messages are wrapped in [Envelope] which carries:
//
//   - Shard: Target shard number
//   - Type: Message type for routing to handlers
//   - Data: JSON-encoded payload
//   - Headers: Optional metadata (use [WithHeader])
//   - TTL: Optional time-to-live (use [WithTTL])
//
// # Error Handling
//
// Common errors include:
//
//   - [ErrTransportNoShardSubscriber]: No node owns the target shard
//   - [ErrEnvelopeExpired]: Message TTL exceeded before delivery
//   - [ErrHandlerTimeout]: Handler exceeded context deadline
package cluster
