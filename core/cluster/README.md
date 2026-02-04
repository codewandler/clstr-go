# Cluster

*Detailed documentation coming soon.*

For now, see the [main README](../../README.md) for an overview.

## Quick Reference

```go
// Create a node
node := cluster.NewNode(
    cluster.WithNodeID("node-1"),
    cluster.WithTransport(transport),
    cluster.WithHandler(handler),
    cluster.WithNumShards(256),
)
node.Start(ctx)
defer node.Shutdown()

// Create a client
client := cluster.NewClient(transport,
    cluster.WithNumShards(256),
)

// Route by key
response, err := client.RequestKey(ctx, "user-123", "GetUser", payload)

// Route by shard directly
response, err := client.RequestShard(ctx, shardID, "GetUser", payload)
```

## Key Types

- `Node` — Cluster node that owns shards and routes to handlers
- `Client` — Smart routing client with key-based and shard-based routing
- `Transport` — Abstraction for cluster communication (NATS, in-memory)
- `Envelope` — Message wrapper with shard, type, and payload

## Features

- Consistent hashing (rendezvous/HRW)
- Deterministic key-to-shard mapping
- Topology-aware routing
- Transport abstraction for testing
