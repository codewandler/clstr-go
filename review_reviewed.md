# Review Validation: review.md

Each claim in the production readiness review was verified against the current source code.
This document records the verdict for every section.

Legend: ✅ Confirmed · ❌ Incorrect · ⚠️ Partially correct

---

## 1. Race Conditions & Concurrency Issues

### 1.1 CRITICAL: NATS EventStore Append is Not Atomic — ✅ Confirmed ✅ Fixed

**Claimed location**: `adapters/nats/store.go:444-491`
**Actual location**: `adapters/nats/store.go:462-508`

The TOCTOU pattern exists exactly as described: `getMostRecentVersionForAgg` → version
comparison → append loop. The code itself acknowledges the weakness at line 487 with the
comment "Optimistic check (best-effort)." No JetStream expected-sequence header is used in
the `append()` call (line 511-543), so two writers can both pass the check and both succeed.

**Fix**: Replaced the best-effort version check with JetStream's
`WithExpectLastSequencePerSubject` header for atomic compare-and-swap on the first event
of each `Append` batch. Switched from `PublishMsgAsync` to synchronous `PublishMsg` so the
CAS rejection is detected immediately and mapped to `ErrConcurrencyConflict`. Removed the
now-unused `getMostRecentVersionForAgg` helper.

### 1.2 HIGH: State Channel Buffer of 1 — ✅ Confirmed

**Claimed location**: `core/actor/v2/state.go:47-56`
**Actual location**: `core/actor/v2/state.go:49-58`

Line 52: `tasks: make(chan StateTask[T], 1)`. The code, impact, and recommendation are all
accurate.

### 1.3 HIGH: InMemorySubscription.dispatch Holds Lock During Channel Send — ✅ Confirmed

**Claimed location**: `core/es/store_memory.go:213-232`
**Actual location**: `core/es/store_memory.go:213-232` (exact match)

`mu.Lock()` at line 214, `defer mu.Unlock()` at line 215, then blocking channel send
`i.ch <- e` at line 230 inside the lock. The review's recommended fix (filter under lock,
send after unlock) is sound.

### 1.4 MEDIUM: LRU Cache Close() Can Panic on Double-Close — ✅ Confirmed ✅ Fixed

**Claimed location**: `core/cache/lru.go:92-94`
**Actual location**: `core/cache/lru.go:92-94` (exact match)

`close(L.doneCh)` with no `sync.Once` protection. **Additional finding the review missed:**
the doc comment on line 91 reads "Close is idempotent" — but the implementation is not.
The comment is a lie. The `sync.Once` fix is the right answer.

**Fix**: Added `sync.Once` to `LRU.Close()` so double-close no longer panics — the doc
comment's idempotency claim is now truthful. Added `TestLRU_Close_Idempotent` covering
double-close and post-close operations.

### 1.5 MEDIUM: perkey.Scheduler Workers Never Cleaned Up — ✅ Confirmed

**Claimed location**: `core/perkey/perkey.go:138-158`
**Actual location**: `core/perkey/perkey.go:138-151`

Workers are added to the map at line 147 and only removed during `Shutdown()` (line 134).
Default buffer size is 64 (line 51: `cfg := &config{bufferSize: 64}`), matching the review.

> Note: This section is numbered 1.5 in the review but appears *after* section 1.6 in the
> document. Minor ordering error.

### 1.6 HIGH: KeyValueSnapshotter Key Collision — ✅ Confirmed ✅ Fixed

**Claimed location**: `core/es/snapshot.go:172`
**Actual location**: `core/es/snapshot.go:172-173` (exact match)

`fmt.Sprintf("%s-%s", objType, objID)` — the `-` separator is ambiguous. The collision
scenario (`"order"` + `"item-99"` = `"order-item"` + `"99"`) is valid and realistic given
UUID-based aggregate IDs.

**Fix**: Changed separator from `-` to `:` (`fmt.Sprintf("%s:%s", ...)`). Added
`TestKeyValueSnapshotter_getKey_NoCollision` and `TestKeyValueSnapshotter_getKey_Format`
to verify the fix and prevent regression.

### 1.7 MEDIUM: Redis Snapshotter Uses Unversioned SET — ✅ Confirmed

**Claimed location**: `adapters/redis/kv.go:61`
**Actual location**: `adapters/redis/kv.go:62`

`s.client.Set(ctx, s.getKey(key), entry.Data, opts.TTL).Err()` — unconditional overwrite.
The self-healing analysis in the review is accurate: ES optimistic concurrency limits the
race window, and event replay corrects the state on next load.

---

## 2. Performance Issues

### 2.1 HIGH: NATS EventStore.Load Creates New Consumer Per Call — ✅ Confirmed

**Claimed location**: `adapters/nats/store.go:309-388`
**Actual location**: `adapters/nats/store.go:327-406`

`Load()` performs: `getMostRecentEventForAgg` (round-trip) → `stream.OrderedConsumer`
(round-trip) → `consumeEvents`. Minimum two network round-trips per load.

### 2.2 HIGH: JSON Marshaling Everywhere — ✅ Confirmed

All four cited locations verified:

| Cited location | Actual location | Code |
|---|---|---|
| `handler.go:133` | `handler.go:133` | `json.Unmarshal(data, &tt)` |
| `client.go:190-191` | `client.go:190` | `json.Marshal(payload)` |
| `repo.go:172` | `repo.go:170` | `json.Marshal(ev)` |
| `snapshot.go:137` | `snapshot.go:137` | `json.Marshal(agg)` |

No `Codec` interface exists. JSON is hardcoded throughout.

### 2.3 MEDIUM: Event Loading Doesn't Support Pagination — ✅ Confirmed ✅ Fixed

**Claimed location**: `adapters/nats/store.go:390-442`
**Actual location**: `adapters/nats/store.go:408-460`
**Commit**: `3dc770d` — `fix(nats): replace FetchNoWait with Fetch in consumeEvents`
**Plan**: `.agents/plans/PLAN-20260408T111156Z.md`

`consumeEvents` at line 408 calls `cc.FetchNoWait(100)` at line 429, appending everything
into a single `[]es.Envelope` slice in memory. No streaming or iterator API.

**Production impact confirmed**: on a 2.8 M-message stream with ~1% subject density,
`FetchNoWait` returned empty after the first 100 events, causing partial aggregate loads,
permanent concurrency conflicts, and an infinite retry loop for two production accounts
(`610822ef`, `124478c1` in the `latest` namespace). In the worst case the ordered
consumer's self-healing reset deadlocked `FetchNoWait` entirely.

**Fix**: replaced `FetchNoWait(100)` with `Fetch(100, FetchMaxWait(e.loadFetchTimeout))`
(default 5 s, configurable via `EventStoreConfig.LoadFetchTimeout`). Dead `startVersion`
local variable removed; TODO comment on `endSeq` resolved.

### 2.4 MEDIUM: Reflection Used in Hot Paths — ✅ Confirmed

**Claimed location**: `core/es/repo.go:294-307`
**Actual location**: `core/es/repo.go:292-306`

`reflect.TypeOf` and `reflect.New` are used in `NewWithID` at lines 297-299. This runs for
every aggregate load unless the aggregate implements `Create() T`.

### 2.5 LOW: Inefficient Shard Distribution — ✅ Confirmed

**Claimed location**: `core/cluster/shard.go:40-70`
**Actual location**: `core/cluster/shard.go:40-69`

Nested loop: O(numShards × numNodes) hashes. Called on startup/rebalance only, so the
impact assessment ("acceptable") is fair.

---

## 3. Error Handling & Resilience

### 3.1 CRITICAL: Consumer Silently Continues After Handler Errors — ✅ Confirmed

**Claimed location**: `core/es/consumer.go:162-165`
**Actual location**: `core/es/consumer.go:191-199`

In `runSubscription`, line 197: `if err := c.handle(ctx, ev); err != nil` → logs the error
at line 198 → continues to next loop iteration. No retry, no DLQ, no circuit breaker.

Line numbers are significantly off (162 vs 191), but the behavior is exactly as described.

### 3.2 HIGH: No Backpressure in NATS Subscription — ✅ Confirmed

**Claimed location**: `adapters/nats/store.go:294-298`
**Actual location**: `adapters/nats/store.go:311-315`

`select { case ch <- *ev: case <-ctx.Done(): return }` — channel buffered at 64
(line 219: `make(chan es.Envelope, 64)`). No flow-control metrics or back-off.

### 3.3 MEDIUM: Transport Errors Lose Type Information — ✅ Confirmed

**Claimed location**: `adapters/nats/transport.go:118-119`
**Actual location**: `adapters/nats/transport.go:118-119` (exact match)

`errors.New(rf.Err)` converts a structured error to a plain string, losing type
information. `errors.Is()` and `errors.As()` will not work across transport boundaries.

### 3.4 MEDIUM: Actor OnPanic Doesn't Support Recovery Decisions — ✅ Confirmed

**Claimed location**: `core/actor/v2/actor.go:284-291`
**Actual location**: `core/actor/v2/actor.go:284-292`

Panic is recovered, `a.onPanic(r, debug.Stack(), msg)` is called (if set), then execution
always continues. No return value, no stop/restart option.

---

## 4. Code Smells & Technical Debt

### 4.1 Duplicate responseFrame Definitions — ✅ Confirmed

**Claimed locations**: `core/cluster/transport_mem.go:21-24`, `adapters/nats/transport.go:35-39`
**Actual locations**: `transport_mem.go:21-24`, `transport.go:36-39`

Identical structs. The NATS copy has a comment: "Must match core/cluster in-memory
transport" — intentional duplication, but duplication nonetheless.

### 4.2 Inconsistent Option Patterns — ✅ Confirmed

Not verified line-by-line, but multiple patterns are visible across the codebase:
functional options (`WithXxx`), struct options (`XxxOpts`), and interface-based options
(`LoadOption`). The observation is fair.

### 4.3 Magic String Message Types — ✅ Confirmed

Message types are derived from Go type names via reflection (see `core/cluster/reflector`
and `core/reflector`). Renaming a type silently breaks message routing.

### 4.4 TODO Comments in Production Code — ✅ Confirmed

| Cited location | Actual location | Content |
|---|---|---|
| `handler.go:77` | `handler.go:77` | `// TODO: use struct` |
| `handler.go:284` | `handler.go:284` | `// TODO: validate` |
| `store.go:357` | `store.go:375` | `// TODO: verify this is actually correct` |

The storage layer TODO is particularly concerning — it guards the end-sequence calculation
used in `Load()`.

### 4.5 Inconsistent Nil Handling — ❌ Incorrect

**Claimed example**: `store.go:452` contains `if events == nil || len(events) == 0 {`
**Actual code**: `store.go:470` contains `if len(events) == 0 {`

The "safe" example (`events == nil || len(events) == 0`) does not exist at the cited
location. The general observation about inconsistency *may* hold elsewhere, but the
specific example is fabricated.

---

## 5. Developer Ergonomics

### 5.1 HIGH: No Aggregate Lifecycle Hooks — ✅ Confirmed

No `BeforeSave`, `AfterLoad`, or `OnCreate` interfaces exist in the `Aggregate` interface
or repository code. The observation is accurate.

### 5.2 HIGH: Difficult to Test Consumer Handlers — ✅ Confirmed

The `Consumer` type requires a full store subscription to process events. No
`ProcessEvent()` helper for unit testing handlers in isolation.

### 5.3 MEDIUM: No Built-in Aggregate Validation — ✅ Confirmed

No `Validatable` interface checked before `Raise()`. Events and aggregates have no
automatic validation hook.

### 5.4 MEDIUM: Stream Filter Logic Bug — ❌ Incorrect

**Claimed code**:
```go
if env.Type != filter.AggregateType {  // Bug: env.Type is EVENT type
```

**Actual code at `stream.go:85-88`**:
```go
func matchFilter(env Envelope, filter SubscribeFilter) bool {
    if filter.AggregateType != "" {
        if env.AggregateType != filter.AggregateType {
            return false
```

The actual code correctly compares `env.AggregateType` against `filter.AggregateType`.
**The review fabricated an incorrect code snippet and reported a bug that does not exist.**
This should be removed from any action items.

### 5.5 LOW: No Migration Support for Snapshots — ⚠️ Partially correct

**Claimed**: SchemaVersion is "Never used."
**Actual**: SchemaVersion is *set* — to `1` in `CreateSnapshot` (snapshot.go:151) and `0`
in `proj.go` (proj.go:118). It is persisted and loaded, but **never read or checked** for
migration purposes. No migration mechanism exists.

More accurate phrasing: "SchemaVersion is stored but never inspected — no migration
mechanism reads it."

### 5.6 LOW: Unclear Ownership Semantics — ✅ Confirmed

`TypedRepository.GetByID` returns the aggregate directly. No documentation on
thread-safety or copy-on-read semantics.

---

## 6. Missing Features

### 6.1–6.8: Feature Gaps — Not code claims; design observations

These sections (Event Upcasting, Saga Support, Projection Rebuild, DLQ, Health Checks,
Graceful Degradation, Aggregate Archival, Multi-Tenancy) are architectural observations,
not code-level claims. They are reasonable assessments of what the framework does not
provide. No verification needed.

---

## 7. Testing Gaps — ✅ Confirmed

The observation that most tests use in-memory implementations is accurate. No chaos/fault
injection tests, no `go test -bench` benchmark suite. A loadtest example exists but is not
a standardized benchmark.

---

## 8. Documentation Gaps — Not code claims; design observations

ADR, runbook, and API compatibility observations are reasonable. No verification needed.

---

## 9. Security Considerations

### 9.1 No Input Validation on Headers — ✅ Confirmed

**Claimed location**: `core/cluster/envelope.go:80-87`
**Actual location**: `core/cluster/envelope.go:80-87` (exact match)

`Validate()` only checks the reserved header prefix. No validation of header values, no
size limits, no content sanitization.

### 9.2 No Rate Limiting — Not a code claim; design observation

Accurate observation. No built-in rate limiting anywhere in the framework.

### 9.3 Sensitive Data in Logs — ✅ Confirmed

**Claimed location**: `core/cluster/node.go:104-113`
**Actual location**: `core/cluster/node.go:104-113` (exact match)

`slog.String("data", string(env.Data))` at line 110 logs the full message payload in error
cases. This will include any PII or secrets in the message body.

---

## 10. Recommendations Summary — Assessment

The recommendations are generally sound, with one exception:

- **Remove "Fix stream filter bug"** from the Immediate list — the bug does not exist (§5.4).
- All other Immediate, Short Term, and Medium Term recommendations are supported by the
  verified findings above.

---

## Summary Table

| Section | Claim | Verdict | Notes |
|---|---|---|---|
| 1.1 | NATS Append TOCTOU | ✅ Confirmed ✅ Fixed | CAS via `ExpectLastSequencePerSubject` |
| 1.2 | State buffer of 1 | ✅ Confirmed | Lines off by 2 |
| 1.3 | dispatch holds lock | ✅ Confirmed | Exact line match |
| 1.4 | LRU double-close panic | ✅ Confirmed ✅ Fixed | `sync.Once` + test |
| 1.5 | perkey workers leak | ✅ Confirmed | |
| 1.6 | Snapshot key collision | ✅ Confirmed ✅ Fixed | Separator `-` → `:` + tests |
| 1.7 | Redis unversioned SET | ✅ Confirmed | Line off by 1 |
| 2.1 | Load creates consumer | ✅ Confirmed | Lines off by ~18 |
| 2.2 | JSON everywhere | ✅ Confirmed | Lines off by 0-2 |
| 2.3 | No pagination | ✅ Confirmed ✅ Fixed | `3dc770d` — FetchNoWait → Fetch |
| 2.4 | Reflection in hot path | ✅ Confirmed | Lines off by 2 |
| 2.5 | Shard O(s×n) | ✅ Confirmed | |
| 3.1 | Consumer swallows errors | ✅ Confirmed | Lines off by ~30 |
| 3.2 | No backpressure | ✅ Confirmed | Lines off by ~17 |
| 3.3 | Transport error types | ✅ Confirmed | Exact line match |
| 3.4 | OnPanic no decision | ✅ Confirmed | Lines off by 1 |
| 4.1 | Duplicate responseFrame | ✅ Confirmed | |
| 4.2 | Inconsistent options | ✅ Confirmed | |
| 4.3 | Magic string types | ✅ Confirmed | |
| 4.4 | TODO comments | ✅ Confirmed | store.go line off by 18 |
| 4.5 | Inconsistent nil check | ❌ Incorrect | Cited code does not exist |
| 5.1 | No lifecycle hooks | ✅ Confirmed | |
| 5.2 | Hard to test consumers | ✅ Confirmed | |
| 5.3 | No validation | ✅ Confirmed | |
| 5.4 | Stream filter bug | ❌ Incorrect | **Bug does not exist** |
| 5.5 | SchemaVersion unused | ⚠️ Partial | Set but never checked |
| 5.6 | Unclear ownership | ✅ Confirmed | |
| 9.1 | No header validation | ✅ Confirmed | Exact line match |
| 9.3 | PII in logs | ✅ Confirmed | Exact line match |

**Totals**: 25 confirmed (4 fixed), 2 incorrect, 1 partially correct.

The review is largely accurate. The two incorrect claims — the fabricated stream filter bug
(§5.4) and the non-existent nil-check example (§4.5) — should be struck from any action
plan derived from this review.
