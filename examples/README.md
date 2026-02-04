# Examples

Working examples demonstrating clstr's pillars in action.

## counter (Cluster + Actor)

A simple distributed counter that demonstrates the Cluster and Actor pillars without Event Sourcing. Designed for interactive exploration.

### What it demonstrates

1. **Cluster routing**
   - Requests routed by counter ID using consistent hashing
   - Multi-node cluster with shard distribution (configurable)
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

### Running

```bash
cd examples/counter
go run .
```

### Usage

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

### Sample output

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

### Performance

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

---

## accounting (All Three Pillars)

A complete example showing all three pillars working together:

- **Cluster**: Routes account operations to the correct node using consistent hashing
- **Actor**: Each account gets its own actor with mailbox isolation
- **Event Sourcing**: Account state is persisted as a sequence of events

### What it demonstrates

1. **Domain modeling with Event Sourcing**
   - `Account` aggregate with `Open`, `Deposit`, and `Withdraw` commands
   - Events: `AccountOpened`, `MoneyDeposited`, `MoneyWithdrawn`
   - State reconstruction by replaying events

2. **Actor-based message handling**
   - One actor per account ID
   - Type-safe request/response handlers
   - Mailbox isolation (no shared state)

3. **Distributed cluster routing**
   - 3 nodes with shard distribution
   - Requests routed by account ID
   - NATS transport for inter-node communication

### Running

```bash
# From the repository root
task test

# Or directly
cd examples/accounting
go run .
```

The demo will:
1. Start a NATS container (via testcontainers)
2. Spin up a 3-node cluster
3. Create accounts with initial balances
4. Perform deposits and withdrawals
5. Show final balances (reconstructed from events)

### Sample output

```
level=INFO msg="starting NATS container..."
level=INFO msg="NATS ready" url=nats://172.17.0.2:4222
level=INFO msg="starting node" nodeID=node-0 shardCount=20
level=INFO msg="starting node" nodeID=node-1 shardCount=20
level=INFO msg="starting node" nodeID=node-2 shardCount=24
level=INFO msg="=== Demo: Event-Sourced Bank Accounts ==="
level=INFO msg="opening accounts..."
level=INFO msg="opened account" node=node-1 account=alice balance=100
level=INFO msg="opened account" node=node-2 account=bob balance=100
level=INFO msg="opened account" node=node-0 account=charlie balance=100
level=INFO msg="performing transactions..."
level=INFO msg=deposited node=node-1 account=alice amount=50 balance=150
level=INFO msg=withdrew node=node-2 account=bob amount=30 balance=70
level=INFO msg=deposited node=node-0 account=charlie amount=200 balance=300
level=INFO msg="querying final balances..."
level=INFO msg="get balance" node=node-1 account=alice balance=150
level=INFO msg="get balance" node=node-2 account=bob balance=70
level=INFO msg="get balance" node=node-0 account=charlie balance=300
level=INFO msg="demo complete, shutting down..."
```

Notice how each account is consistently routed to the same node (alice->node-1, bob->node-2, charlie->node-0) thanks to consistent hashing.

### Code structure

```go
// 1. Define your aggregate (Event Sourcing)
type Account struct {
    es.BaseAggregate
    balance int
}

func (a *Account) Deposit(amount int) error {
    return es.RaiseAndApply(a, &MoneyDeposited{Amount: amount})
}

// 2. Define command handlers (Actor)
actor.HandleRequest[DepositMoney, BalanceResponse](
    func(hc actor.HandlerCtx, cmd DepositMoney) (*BalanceResponse, error) {
        acc, _ := repo.GetByID(hc.Context(), accountID)
        acc.Deposit(cmd.Amount)
        repo.Save(hc.Context(), acc)
        return &BalanceResponse{Balance: acc.Balance()}, nil
    },
)

// 3. Route requests via cluster (Cluster)
cluster.NewRequest[DepositMoney, BalanceResponse](client.Key("alice")).
    Request(ctx, DepositMoney{Amount: 50})
```
