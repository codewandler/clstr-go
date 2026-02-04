# Prometheus Adapter

Prometheus implementations of the metrics interfaces for all three clstr pillars (ES, Actor, Cluster).

## Overview

This adapter provides production-ready Prometheus metrics for observing your clstr-based applications. All metrics follow Prometheus naming conventions and use appropriate metric types (Counter, Gauge, Histogram).

## Import

```go
import promadapter "github.com/codewandler/clstr-go/adapters/prometheus"
```

## Quick Start

### Combined Usage (Recommended)

When using all three pillars together:

```go
import (
    "github.com/prometheus/client_golang/prometheus"
    promadapter "github.com/codewandler/clstr-go/adapters/prometheus"
)

// Create metrics for all pillars at once
metrics := promadapter.NewAllMetrics(prometheus.DefaultRegisterer)

// Use with ES
env := es.NewEnv(
    es.WithMetrics(metrics.ES),
    // ... other options
)

// Use with Actor
a := actor.New(actor.Options{
    Metrics: metrics.Actor,
    // ... other options
}, handler)

// Use with Cluster
client, _ := cluster.NewClient(cluster.ClientOptions{
    Metrics: metrics.Cluster,
    // ... other options
})

node := cluster.NewNode(cluster.NodeOptions{
    Metrics: metrics.Cluster,
    // ... other options
})
```

### Standalone Usage

For using individual pillars:

```go
// ES only
esMetrics := promadapter.NewESMetrics(prometheus.DefaultRegisterer)
env := es.NewEnv(es.WithMetrics(esMetrics), ...)

// Actor only
actorMetrics := promadapter.NewActorMetrics(prometheus.DefaultRegisterer)
a := actor.New(actor.Options{Metrics: actorMetrics, ...}, handler)

// Cluster only
clusterMetrics := promadapter.NewClusterMetrics(prometheus.DefaultRegisterer)
client, _ := cluster.NewClient(cluster.ClientOptions{Metrics: clusterMetrics, ...})
```

### Custom Registry

For isolated metric registries (useful in tests):

```go
reg := prometheus.NewRegistry()
metrics := promadapter.NewAllMetrics(reg)

// Expose via HTTP
http.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
```

## ES Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `clstr_es_store_load_duration_seconds` | Histogram | `aggregate_type` | Event store load latency |
| `clstr_es_store_append_duration_seconds` | Histogram | `aggregate_type` | Event store append latency |
| `clstr_es_events_appended_total` | Counter | `aggregate_type` | Total events appended |
| `clstr_es_repo_load_duration_seconds` | Histogram | `aggregate_type` | Repository load latency |
| `clstr_es_repo_save_duration_seconds` | Histogram | `aggregate_type` | Repository save latency |
| `clstr_es_concurrency_conflicts_total` | Counter | `aggregate_type` | Optimistic lock failures |
| `clstr_es_cache_hits_total` | Counter | `aggregate_type` | TypedRepository cache hits |
| `clstr_es_cache_misses_total` | Counter | `aggregate_type` | TypedRepository cache misses |
| `clstr_es_snapshot_load_duration_seconds` | Histogram | `aggregate_type` | Snapshot load latency |
| `clstr_es_snapshot_save_duration_seconds` | Histogram | `aggregate_type` | Snapshot save latency |
| `clstr_es_consumer_event_duration_seconds` | Histogram | `event_type`, `live` | Event processing time |
| `clstr_es_consumer_events_total` | Counter | `event_type`, `live`, `success` | Events processed |
| `clstr_es_consumer_lag` | Gauge | `consumer` | Consumer lag (sequences behind head) |

## Actor Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `clstr_actor_message_duration_seconds` | Histogram | `message_type` | Message handling time |
| `clstr_actor_messages_total` | Counter | `message_type`, `success` | Messages processed |
| `clstr_actor_panics_total` | Counter | `message_type` | Handler panics (recovered) |
| `clstr_actor_mailbox_depth` | Gauge | `actor_id` | Current mailbox queue depth |
| `clstr_actor_scheduler_inflight` | Gauge | `actor_id` | Concurrent scheduled tasks |
| `clstr_actor_scheduler_task_duration_seconds` | Histogram | - | Scheduled task duration |
| `clstr_actor_scheduler_tasks_total` | Counter | `success` | Scheduled tasks completed |

## Cluster Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `clstr_cluster_request_duration_seconds` | Histogram | `message_type` | Client request latency |
| `clstr_cluster_requests_total` | Counter | `message_type`, `success` | Client requests sent |
| `clstr_cluster_notifies_total` | Counter | `message_type`, `success` | Client notifications sent |
| `clstr_cluster_transport_errors_total` | Counter | `error_type` | Transport errors by type |
| `clstr_cluster_handler_duration_seconds` | Histogram | `message_type` | Handler execution time |
| `clstr_cluster_handlers_total` | Counter | `message_type`, `success` | Handlers executed |
| `clstr_cluster_handlers_active` | Gauge | `node_id` | Concurrent active handlers |
| `clstr_cluster_shards_owned` | Gauge | `node_id` | Shards owned by node |

**Transport Error Types:**
- `no_subscriber` — No handler registered for shard
- `timeout` — Handler exceeded deadline
- `ttl_expired` — Request TTL exceeded before processing
- `closed` — Transport was closed

## Histogram Buckets

All duration histograms use the following bucket boundaries (in seconds):

```
.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10
```

These buckets are optimized for typical microservice latencies, covering sub-millisecond to multi-second operations.

## Example: Full Application

```go
package main

import (
    "context"
    "log/slog"
    "net/http"

    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promhttp"

    promadapter "github.com/codewandler/clstr-go/adapters/prometheus"
    "github.com/codewandler/clstr-go/core/actor/v2"
    "github.com/codewandler/clstr-go/core/cluster"
    "github.com/codewandler/clstr-go/core/es"
)

func main() {
    ctx := context.Background()

    // Initialize Prometheus metrics
    metrics := promadapter.NewAllMetrics(prometheus.DefaultRegisterer)

    // Expose metrics endpoint
    http.Handle("/metrics", promhttp.Handler())
    go http.ListenAndServe(":9090", nil)

    // Create ES environment with metrics
    env := es.NewEnv(
        es.WithInMemory(),
        es.WithMetrics(metrics.ES),
        es.WithAggregates(&MyAggregate{}),
    )
    env.Start()
    defer env.Shutdown()

    // Create actor with metrics
    a := actor.TypedHandlers(
        actor.HandleMsg[MyMessage](handleMessage),
    ).ToActor(actor.Options{
        Context: ctx,
        Metrics: metrics.Actor,
    })

    // Create cluster with metrics
    tr := cluster.NewInMemoryTransport()
    client, _ := cluster.NewClient(cluster.ClientOptions{
        Transport: tr,
        NumShards: 256,
        Metrics:   metrics.Cluster,
    })

    // ... use env, actor, client
}
```

## Grafana Dashboards

Example PromQL queries for building dashboards:

```promql
# Request rate by message type
rate(clstr_cluster_requests_total[5m])

# P99 latency for repository saves
histogram_quantile(0.99, rate(clstr_es_repo_save_duration_seconds_bucket[5m]))

# Actor message processing rate
sum(rate(clstr_actor_messages_total[5m])) by (message_type)

# Consumer lag
clstr_es_consumer_lag

# Error rate
rate(clstr_cluster_requests_total{success="false"}[5m])
  / rate(clstr_cluster_requests_total[5m])
```

## Zero-Overhead Default

When no metrics are provided, all pillars use a no-op implementation with zero overhead. This means you can optionally enable metrics without any performance impact when disabled.
