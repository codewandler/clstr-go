# Counter Example

A simple distributed counter demonstrating the **Cluster** and **Actor** pillars without Event Sourcing. Designed for interactive exploration.

## What it demonstrates

1. **Cluster routing**
   - Requests routed by counter ID using consistent hashing
   - Multi-node cluster with shard distribution (7 nodes, 64 shards)
   - NATS transport for inter-node communication

2. **Actor-based message handling**
   - One actor per counter ID
   - In-memory state (counter value)
   - Type-safe request/response handlers

3. **Prometheus metrics**
   - Metrics exposed on port 2121
   - Actor and Cluster metrics enabled

4. **HTTP API**
   - Simple REST API on port 8181
   - Easy to test with curl

## Running

```bash
cd examples/counter
go run .
```

## Usage

Once running, interact with the counter:

```bash
# Increment a counter (default +1)
curl -X POST localhost:8181/counter/my-counter/increment

# Increment by a specific amount
curl -X POST localhost:8181/counter/my-counter/increment -d '{"amount": 5}'

# Get current value
curl localhost:8181/counter/my-counter

# View Prometheus metrics
curl localhost:2121/metrics
```

## Sample output

```
level=INFO msg="prometheus metrics server starting" port=2121
level=INFO msg="starting NATS container..."
level=INFO msg="NATS ready" url=nats://172.17.0.2:4222
level=INFO msg="starting node" nodeID=node-0 shardCount=10
level=INFO msg="starting node" nodeID=node-1 shardCount=14
...
level=INFO msg="HTTP server starting" port=8181
level=INFO msg="=== Counter Demo Ready ==="
```

## Performance

Benchmark results on a local machine with NATS running in Docker (7 nodes, 64 shards):

| Metric | Value |
|--------|-------|
| **Throughput** | ~43,000 req/sec |
| **p50 latency** | 11ms |
| **p99 latency** | 20ms |
| **Actor handler time** | ~8μs per message |
| **Memory** | ~12MB heap after 500k requests |

```bash
# Run your own benchmark
hey -n 100000 -c 200 -m POST http://localhost:8181/counter/bench/increment

# Verify correctness
curl localhost:8181/counter/bench
# {"counter_id":"bench","value":100000}
```

The counter value always matches the request count - no lost updates, no duplicates.

## Code structure

```go
// 1. Define messages
type Increment struct {
    Amount int `json:"amount,omitempty"`
}

type CounterResponse struct {
    CounterID string `json:"counter_id"`
    Value     int    `json:"value"`
}

// 2. Create actor handler with in-memory state
cluster.NewActorHandler(func(counterID string) (actor.Actor, error) {
    value := 0  // State captured in closure

    return actor.New(
        actor.Options{...},
        actor.TypedHandlers(
            actor.HandleRequest[Increment, CounterResponse](
                func(hc actor.HandlerCtx, cmd Increment) (*CounterResponse, error) {
                    value += cmd.Amount
                    return &CounterResponse{CounterID: counterID, Value: value}, nil
                },
            ),
        ),
    ), nil
})

// 3. Route requests via cluster
cluster.NewRequest[Increment, CounterResponse](client.Key("my-counter")).
    Request(ctx, Increment{Amount: 1})
```
