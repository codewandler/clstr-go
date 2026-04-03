# Changelog

All notable changes to this project will be documented in this file.

## [v0.32.0] — 2026-04-03

### Added

#### Redis KV adapter (`adapters/redis`)

- **`KvStore`** — implements `ports/kv.Store` backed by `go-redis/v9`.
  `Put` supports per-key TTL via Redis `SET … PX`; `Get` maps `redis.Nil`
  to `kv.ErrNotFound`; `Delete` is a no-op when the key is absent.
  Key namespacing is handled by the `Config.KeyPrefix` field
  (`"{prefix}:{key}"`), consistent with Redis conventions.

- **`NewSnapshotter(cfg Config)`** — wraps `KvStore` in
  `es.NewKeyValueSnapshotter`, providing a drop-in replacement for
  `adapters/nats.NewSnapshotter`. Moves ES aggregate and projection snapshot
  storage to Redis, reducing NATS JetStream KV pressure under high concurrency.

- **`NewTestClient(t Testing) *redis.Client`** — test helper that spins up a
  `redis:7-alpine` testcontainer and returns a connected client, mirroring
  `adapters/nats.NewTestContainer`.

- **`go-redis/v9`** added as a new module dependency. The dependency is only
  pulled in by consumers that import `adapters/redis`; nothing in `core/` or
  `adapters/nats/` is affected.

## [v0.31.0] — 2026-04-02

### Added

#### Self-Healing Consumer (`core/es`)

- **Automatic subscription retry with exponential backoff.** When the underlying
  subscription fails (e.g. NATS server restart, network partition, JetStream consumer
  deletion), the `Consumer` now re-subscribes automatically instead of terminating the
  process. Backoff starts at 1 s (configurable) and doubles up to a configurable maximum
  (default 30 s), resetting on successful reconnection.

- **`Died()` lifecycle channel.** `Consumer.Died()` returns a `<-chan struct{}` that is
  closed when the consumer goroutine exits for any reason — explicit `Stop()`, context
  cancellation, or failed `Start()`. Callers can use it to observe consumer lifecycle
  completion without polling.

- **`WithReconnectBackoff(initial, max)` option.** Configures the exponential backoff
  parameters for subscription retries. `initial` is clamped to a minimum of 100 ms;
  `max` is clamped to at least `initial`.

- **`Done()` on Subscription interface.** `Subscription` now exposes a `Done() <-chan error`
  channel. It is closed on clean shutdown (context cancelled, `Cancel()` called) and sends
  a non-nil error when the subscription fails unexpectedly. This is the primary signal that
  triggers the consumer's self-healing retry loop.

- **`Env.Done()` accessor.** Returns the environment's internal done channel, allowing
  external code to observe when the environment has fully shut down.

#### Observability

- **`ConsumerReconnects` metric.** New method on `ESMetrics` interface, incremented each
  time a consumer re-subscribes after a subscription failure. Useful for alerting on
  flapping connections.
  - Prometheus implementation: `clstr_es_consumer_reconnects_total` counter
    (label: `consumer`).
  - No-op implementation updated.

- **Lazy `MsgCtx.Log()`.** The event-annotated `slog.Logger` is now constructed on first
  call to `Log()` rather than eagerly per event. Eliminates a `slog.With()` heap allocation
  for every event when the handler doesn't log.

- **Faster NATS disconnect detection.** Added `PingInterval(5s)` and
  `MaxPingsOutstanding(2)` to the default NATS connection options, reducing disconnect
  detection from TCP-timeout-dependent (~minutes) to ~10 seconds.

- **`PullExpiry(5s)` on NATS subscription.** Caps each JetStream pull request at 5 seconds
  instead of the default 30 s. A stale iterator (e.g. after a server restart that wiped
  JetStream state) now surfaces an error through `sub.Done()` within 5 seconds.

#### Middleware Options

- **`SetMiddlewareOption` / `WithMiddlewares` (replace semantics).** `WithMiddlewares()`
  now replaces the entire middleware list rather than appending. This is a **breaking change**
  to the return type (now `SetMiddlewareOption` instead of `MiddlewareOption`) but call-site
  code using `WithMiddlewares(...)` continues to compile.

- **`WithMiddlewaresAppend` (append semantics).** Explicitly appends middlewares to the
  existing list. The old `WithMiddlewaresAppend` had a duplication bug (`append(mws, mws...)`)
  which has been fixed.

#### Tests

- `core/es/consumer_test.go` — 11 unit tests: happy path, pre-live retry, closed-channel
  regression, backoff timing, checkpoint resume after retry, idempotent Start, concurrent
  Start with lifecycle failure, lifecycle Start failure deadlock prevention, Done error
  after live.
- `core/es/consumer_died_test.go` — 2 tests: `Died()` closed on post-live retry,
  `Died()` closed on clean stop.
- `core/es/consumer_opts_test.go` — 2 internal tests: `WithMiddlewares` replaces,
  `WithMiddlewaresAppend` extends.
- `core/es/env_test.go` — 2 integration tests: consumer self-heals after failure within
  Env, watchdog does not cancel context on clean stop.
- `core/es/stream_test.go` — 2 tests: `matchFilter` uses `AggregateType` (regression),
  `matchFilter` uses `AggregateID`.
- `adapters/nats/store_test.go` — 2 tests: connection close signals `Done()`,
  server-side consumer deletion signals `Done()`.
- `adapters/nats/reconnect_test.go` — 2 Toxiproxy integration tests: subscription
  behaviour during transient outage, subscription behaviour after full server restart
  with fresh JetStream state.
- `adapters/prometheus/prometheus_test.go` — extended to cover `ConsumerReconnects`.

### Fixed

- **Hot-loop spin on closed subscription channel.** When a subscription channel was closed
  without signalling `Done()` (e.g. non-compliant `Subscription` implementation or an
  ordering race), the consumer goroutine would spin reading zero-value `Envelope`s
  indefinitely. Now, a defensive zero-value guard (`ev.ID == "" && ev.Type == ""`) detects
  the closed channel and returns an error to trigger the retry loop.

- **`matchFilter` compared `env.Type` instead of `env.AggregateType`.** The stream filter
  function used the event type field instead of the aggregate type field when matching
  `SubscribeFilter.AggregateType`, causing filters to silently pass or reject the wrong
  events.

- **`WithMiddlewaresAppend` doubled the middleware list.** The old implementation was
  `append(mws, mws...)` which duplicated every middleware. Now correctly returns
  `MiddlewareOption{v: mws}` (append semantics come from `applyToConsumerOpts`).

- **`Start()` blocked forever when subscription failed before going live.** Previously,
  if the subscription goroutine exited before the consumer went live, `Start()` would
  block on `<-c.live` indefinitely. Now `Start()` selects on `c.live`, `c.done`, and
  `ctx.Done()` with a priority select pattern that correctly handles the race between
  live and done signals.

- **Double-close panic on `c.live` channel.** Live detection now uses `liveOnce sync.Once`
  to guard all `close(c.live)` call sites, preventing a panic when live is triggered from
  multiple code paths.

- **Shutdown timeout used cancelled context.** `HandlerLifecycleShutdown` previously
  received a `context.WithTimeout(ctx, ...)` where `ctx` was already cancelled (e.g.
  during env shutdown). Now uses `context.Background()` as the parent so the shutdown
  timeout is always fresh.

- **`Start()` was not idempotent.** Concurrent calls to `Start()` could launch multiple
  goroutines and run `HandlerLifecycleStart` multiple times. Now protected by `startOnce`:
  the lifecycle callback and goroutine launch happen exactly once, and all callers receive
  the same error.

- **NATS subscription goroutine closed `ch` without signalling `Done()`.** The NATS
  `jsStoreSubscription` goroutine would `close(ch)` on error without any signal, leaving
  the consumer to spin on zero-value reads. Now sends the error to `done` before closing
  `ch`, and closes `done` cleanly on normal exit.

### Changed

- `Consumer` struct is now single-use: `Start()` may be called at most once via
  `startOnce`. Subsequent calls share the result. After `Stop()`, create a new consumer.
- `MsgCtx` struct fields reorganised: `log` replaced by `baseLog` + lazy `log`.
- `checkpointHandler` middleware now calls `msgCtx.Log()` (method) instead of accessing
  `msgCtx.log` (field) directly, compatible with the lazy logger pattern.
- NATS `jsStoreSubscription` now has a `done chan error` field and implements `Done()`.
- `InMemoryStore` subscription now has a `done chan error` field (never written to).

### Chore

- `.gitignore` updated to exclude `.coder/plans` directory.
- `TODO.md` updated to reflect current project state.
