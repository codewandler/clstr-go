# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Documentation

- **[README.md](./README.md)** — Project overview and why the three pillars work together
- **[core/es/README.md](./core/es/README.md)** — Event Sourcing package documentation
- **[core/actor/README.md](./core/actor/README.md)** — Actor Model package documentation
- **[core/cluster/README.md](./core/cluster/README.md)** — Cluster package documentation
- **[core/app/README.md](./core/app/README.md)** — High-level App convenience wrapper
- **[adapters/prometheus/README.md](./adapters/prometheus/README.md)** — Prometheus metrics adapter

## Build/Test Commands

```bash
task test      # Run all tests with race detection: go test -v -count=1 -race -failfast ./...
task fix       # Format code with goimports: goimports -v -w -local "$(go list -m)" .
task           # Run both test + fix
task loadtest  # Run performance tests (configurable via env: N, B, BACKEND, SNAPSHOT, CACHE, LOAD_AFTER_SAVE)
```

Run a single test:
```bash
go test -v -count=1 -race -run TestName ./path/to/package/...
```

## Architecture Overview

This is a distributed **event sourcing framework** for Go with NATS JetStream as the primary storage backend.

### Core Packages (`core/`)

- **`es/`** - Event Sourcing: Aggregate base, EventStore interface, Repository with optimistic concurrency, Consumer with checkpointing, Snapshot support, Environment factory
- **`actor/v2/`** - Actor Model: BaseActor with mailbox-based message handling, typed handlers, periodic tasks (HandleEvery), Scheduler for concurrent background tasks, self-request deadlock prevention
- **`cluster/`** - Clustering: Node coordination, Client for shard communication, consistent hashing for shard distribution
- **`app/`** - High-level orchestration combining cluster + actor patterns
- **`cache/`** - Cache interface with LRU and no-op implementations
- **`perkey/`** - Per-key serialization scheduler (sequential per key, concurrent across keys)
- **`sf/`** - Single-flight pattern
- **`ds/`** - Generic data structures (ordered Set)
- **`reflector/`** - Type reflection utilities for event/message type naming
- **`metrics/`** - Pluggable metrics interfaces (Counter, Gauge, Histogram, Timer) with no-op defaults

### Adapters (`adapters/`)

- **`nats/`** - NATS JetStream: EventStore, KV store (snapshots/checkpoints), connection management, cluster transport
- **`prometheus/`** - Prometheus metrics: implementations of ESMetrics, ActorMetrics, ClusterMetrics for all three pillars

### Ports (`ports/`)

- **`kv/`** - Key-value store abstraction with in-memory implementation

## Key Patterns

**Event Sourcing Flow:**
```
Aggregate -> raises events -> EventStore -> Consumers -> Projections
                                        -> Snapshots (optional)
```

**TypedRepository usage:**
```go
repo := es.NewTypedRepositoryFrom[*User](log, env.Repository())
user, _ := repo.GetByID(ctx, userID)
user.ChangeEmail(...)
repo.Save(ctx, user)
```

**Testing with test environment:**
```go
env := es.StartTestEnv(t, options...)
// env provides: Store(), Repository(), Registry(), NewConsumer()
```

## Conventions

- Use Mermaid for diagrams in documentation (GitHub renders it natively)
- `WithXxx()` for option functions
- `NewXxx()` for constructors
- `Typed*` prefix for generic wrappers (e.g., `TypedRepository[T]`)
- `Start()`/`Shutdown()` for lifecycle management
- Context-based cancellation throughout
- `log/slog` for structured logging
- Optimistic concurrency via `Version` type
- Error types: `ErrAggregateNotFound`, `ErrConcurrencyConflict`, `ErrSelfRequest`
