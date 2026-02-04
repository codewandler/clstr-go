# Load Test Example

A performance benchmark for the **Event Sourcing** pillar, measuring throughput and latency of aggregate operations.

## What it demonstrates

1. **Event Sourcing performance**
   - Load/save cycles with optimistic concurrency
   - Snapshot optimization impact
   - Event replay performance

2. **Backend comparison**
   - NATS JetStream (production)
   - In-memory (baseline)

3. **Caching strategies**
   - LRU cache effectiveness
   - Memory vs throughput tradeoffs

## Running

```bash
cd examples/loadtest
go run .
```

The benchmark will:
1. Start a NATS container (via testcontainers)
2. Run N aggregate operations across U unique users
3. Report progress every B operations
4. Print final throughput and memory stats

## Configuration

All settings are configurable via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `N` | 50000 | Total number of operations |
| `U` | 100 | Number of unique users (aggregate keys) |
| `B` | 1000 | Batch size for progress reporting |
| `BACKEND` | nats | Backend: `nats` or `mem` |
| `SNAPSHOT` | true | Enable snapshot optimization |
| `CACHE` | false | Enable LRU aggregate cache |
| `LOAD_AFTER_SAVE` | false | Reload aggregate after each save |

### Examples

```bash
# Quick test with in-memory backend
BACKEND=mem N=10000 go run .

# Test without snapshots (replay all events)
SNAPSHOT=false N=10000 go run .

# Test with caching enabled
CACHE=true N=50000 go run .

# Full verification mode (load after each save)
LOAD_AFTER_SAVE=true N=10000 go run .
```

## Sample output

```
=== Load Test Configuration ===
  Backend:         nats
  Total ops:       50000
  Unique users:    100
  Batch size:      1000
  Snapshots:       true
  Cache:           false
  Load after save: false

level=INFO msg="starting NATS container..."
level=INFO msg="NATS ready" url=nats://172.17.0.2:4222
level=INFO msg="=== Starting Load Test ==="
.......... |  1000 ops |    892 ms |   1121 ops/s | 24/36 MiB (heap/sys)
.......... |  1000 ops |    756 ms |   1323 ops/s | 26/36 MiB (heap/sys)
.......... |  1000 ops |    743 ms |   1346 ops/s | 28/36 MiB (heap/sys)
...

=== Results ===
  Total time:    38.234 seconds
  Total ops:     50000
  Avg rate:      1308 ops/s
  Final memory:  18 MiB heap, 36 MiB sys
```

## Interpreting results

- **ops/s** - Higher is better; affected by backend, network, and snapshot frequency
- **heap/sys** - Memory usage; heap grows with aggregate cache, sys is total from OS
- **Batch variance** - Large swings may indicate GC pauses or snapshot writes

### Typical performance

| Configuration | Throughput | Notes |
|---------------|------------|-------|
| NATS + snapshots | ~1,300 ops/s | Production config |
| NATS no snapshots | ~800 ops/s | Event replay overhead |
| In-memory | ~50,000 ops/s | Baseline (no I/O) |
| NATS + cache | ~2,000 ops/s | Reduced reads |

## Code structure

```go
// 1. Define aggregate with events
type User struct {
    es.BaseAggregate
    Email string
}

func (u *User) ChangeEmail(email string) error {
    return es.RaiseAndApply(u, &EmailChanged{NewEmail: email})
}

// 2. Load, modify, save cycle
user, _ := repo.GetOrCreate(ctx, userID, es.WithSnapshot(true))
user.ChangeEmail("new@example.com")
repo.Save(ctx, user, es.WithSnapshot(true))
```

## Task integration

This benchmark is also available via the task runner:

```bash
task loadtest                    # Default settings
N=100000 CACHE=true task loadtest  # Custom settings
```
