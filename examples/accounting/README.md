# Accounting Example

A complete example showing all three pillars working together:

- **Cluster**: Routes account operations to the correct node using consistent hashing
- **Actor**: Each account gets its own actor with mailbox isolation
- **Event Sourcing**: Account state is persisted as a sequence of events

## What it demonstrates

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

## Running

```bash
cd examples/accounting
go run .
```

The demo will:
1. Start a NATS container (via testcontainers)
2. Spin up a 3-node cluster
3. Create accounts with initial balances
4. Perform deposits and withdrawals
5. Show final balances (reconstructed from events)

## Sample output

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

Notice how each account is consistently routed to the same node (alice→node-1, bob→node-2, charlie→node-0) thanks to consistent hashing.

## Code structure

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
