// Package main demonstrates a distributed counter using the Cluster and Actor pillars.
//
// This example shows how to use clstr without Event Sourcing:
//   - Cluster: Routes requests to the correct node based on counter ID
//   - Actor: Processes messages with mailbox isolation (one actor per counter)
//
// Run with: go run .
// Then use curl to interact:
//
//	curl -X POST localhost:8181/counter/my-counter/increment
//	curl localhost:8181/counter/my-counter
//
// Prometheus metrics available at: http://localhost:2121/metrics
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/codewandler/clstr-go/adapters/nats"
	promadapter "github.com/codewandler/clstr-go/adapters/prometheus"
	"github.com/codewandler/clstr-go/core/actor/v2"
	"github.com/codewandler/clstr-go/core/cluster"
)

// =============================================================================
// Messages
// =============================================================================

type (
	// Increment increases the counter by the specified amount (default 1).
	Increment struct {
		Amount int `json:"amount,omitempty"`
	}

	// GetValue retrieves the current counter value.
	GetValue struct{}

	// CounterResponse is the response for counter operations.
	CounterResponse struct {
		CounterID string `json:"counter_id"`
		Value     int    `json:"value"`
	}
)

// =============================================================================
// Configuration
// =============================================================================

const (
	numNodes  = 7
	numShards = 64
	seed      = "counter-cluster"
	httpPort  = 8181
	promPort  = 2121
)

// =============================================================================
// Main
// =============================================================================

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)

	if err := run(ctx, log); err != nil {
		log.Error("demo failed", slog.Any("error", err))
		os.Exit(1)
	}
}

func run(ctx context.Context, log *slog.Logger) error {
	// Initialize Prometheus metrics
	metrics := promadapter.NewAllMetrics(prometheus.DefaultRegisterer)

	// Start Prometheus metrics server
	promMux := http.NewServeMux()
	promMux.Handle("/metrics", promhttp.Handler())
	promServer := &http.Server{Addr: fmt.Sprintf(":%d", promPort), Handler: promMux}
	go func() {
		log.Info("prometheus metrics server starting", slog.Int("port", promPort))
		if err := promServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("prometheus server error", slog.Any("error", err))
		}
	}()
	defer promServer.Shutdown(context.Background())

	// Start NATS container
	log.Info("starting NATS container...")
	natsURL, cleanup, err := startNATSContainer(ctx, log)
	if err != nil {
		return fmt.Errorf("start nats: %w", err)
	}
	defer cleanup()
	log.Info("NATS ready", slog.String("url", natsURL))

	// Create transports for each node
	transports := make([]*nats.Transport, numNodes)
	for i := range numNodes {
		tr, err := nats.NewTransport(nats.TransportConfig{
			Connect:       nats.ConnectURL(natsURL),
			Log:           log.With(slog.Int("node", i)),
			SubjectPrefix: "counter",
		})
		if err != nil {
			return fmt.Errorf("create transport %d: %w", i, err)
		}
		defer tr.Close()
		transports[i] = tr
	}

	// Generate node IDs
	nodeIDs := make([]string, numNodes)
	for i := range numNodes {
		nodeIDs[i] = fmt.Sprintf("node-%d", i)
	}

	// Start cluster nodes
	var wg sync.WaitGroup
	nodeCtx, nodeCancel := context.WithCancel(ctx)
	defer nodeCancel()

	for i := range numNodes {
		nodeID := nodeIDs[i]
		shards := cluster.ShardsForNode(nodeID, nodeIDs, numShards, seed)
		log.Info("starting node",
			slog.String("nodeID", nodeID),
			slog.Int("shardCount", len(shards)),
		)

		node := cluster.NewNode(cluster.NodeOptions{
			NodeID:    nodeID,
			Transport: transports[i],
			Shards:    shards,
			Log:       log.With(slog.String("node", nodeID)),
			Handler:   createCounterHandler(nodeCtx, log.With(slog.String("node", nodeID)), metrics.Actor),
			Metrics:   metrics.Cluster,
		})

		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := node.Run(nodeCtx); err != nil && nodeCtx.Err() == nil {
				log.Error("node error", slog.Any("error", err))
			}
		}()
	}

	// Give nodes time to subscribe
	time.Sleep(100 * time.Millisecond)

	// Create a client for the HTTP server
	client, err := cluster.NewClient(cluster.ClientOptions{
		Transport: transports[0],
		NumShards: numShards,
		Seed:      seed,
		Metrics:   metrics.Cluster,
	})
	if err != nil {
		return fmt.Errorf("create client: %w", err)
	}

	// Start HTTP server
	httpMux := http.NewServeMux()
	httpMux.HandleFunc("POST /counter/{id}/increment", handleIncrement(client))
	httpMux.HandleFunc("GET /counter/{id}", handleGetValue(client))
	httpMux.HandleFunc("GET /", handleIndex)

	httpServer := &http.Server{Addr: fmt.Sprintf(":%d", httpPort), Handler: httpMux}
	go func() {
		log.Info("HTTP server starting", slog.Int("port", httpPort))
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("http server error", slog.Any("error", err))
		}
	}()
	defer httpServer.Shutdown(context.Background())

	log.Info("=== Counter Demo Ready ===")
	log.Info("try these commands:",
		slog.String("increment", fmt.Sprintf("curl -X POST localhost:%d/counter/my-counter/increment", httpPort)),
		slog.String("get", fmt.Sprintf("curl localhost:%d/counter/my-counter", httpPort)),
		slog.String("metrics", fmt.Sprintf("http://localhost:%d/metrics", promPort)),
	)
	log.Info("press Ctrl+C to stop")

	// Wait for shutdown signal
	<-ctx.Done()
	log.Info("shutting down...")

	nodeCancel()
	wg.Wait()

	return nil
}

// =============================================================================
// HTTP Handlers
// =============================================================================

func handleIndex(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintf(w, `Counter Demo

Available endpoints:
  POST /counter/{id}/increment  - Increment a counter (body: {"amount": N} or empty for +1)
  GET  /counter/{id}            - Get current counter value

Examples:
  curl -X POST localhost:%d/counter/my-counter/increment
  curl -X POST localhost:%d/counter/my-counter/increment -d '{"amount": 5}'
  curl localhost:%d/counter/my-counter

Prometheus metrics:
  http://localhost:%d/metrics
`, httpPort, httpPort, httpPort, promPort)
}

func handleIncrement(client *cluster.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		counterID := r.PathValue("id")
		if counterID == "" {
			http.Error(w, "counter ID required", http.StatusBadRequest)
			return
		}

		var inc Increment
		if r.ContentLength > 0 {
			if err := json.NewDecoder(r.Body).Decode(&inc); err != nil {
				http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
				return
			}
		}
		if inc.Amount == 0 {
			inc.Amount = 1
		}

		resp, err := cluster.NewRequest[Increment, CounterResponse](client.Key(counterID)).
			Request(r.Context(), inc)
		if err != nil {
			http.Error(w, "cluster error: "+err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}
}

func handleGetValue(client *cluster.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		counterID := r.PathValue("id")
		if counterID == "" {
			http.Error(w, "counter ID required", http.StatusBadRequest)
			return
		}

		resp, err := cluster.NewRequest[GetValue, CounterResponse](client.Key(counterID)).
			Request(r.Context(), GetValue{})
		if err != nil {
			http.Error(w, "cluster error: "+err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}
}

// =============================================================================
// Actor Handler
// =============================================================================

// createCounterHandler returns a cluster handler that spawns one actor per counter.
// Each actor maintains its counter value in memory.
func createCounterHandler(ctx context.Context, log *slog.Logger, actorMetrics actor.ActorMetrics) cluster.ServerHandlerFunc {
	return cluster.NewActorHandler(func(counterID string) (actor.Actor, error) {
		// Counter state - captured in closure, isolated per actor
		value := 0

		return actor.New(
			actor.Options{
				Context:     ctx,
				Logger:      log.With(slog.String("counter", counterID)),
				MailboxSize: 256,
				Metrics:     actorMetrics,
			},
			actor.TypedHandlers(
				actor.HandleRequest[Increment, CounterResponse](
					func(hc actor.HandlerCtx, cmd Increment) (*CounterResponse, error) {
						value += cmd.Amount
						hc.Log().Info("incremented", slog.Int("amount", cmd.Amount), slog.Int("value", value))
						return &CounterResponse{CounterID: counterID, Value: value}, nil
					},
				),

				actor.HandleRequest[GetValue, CounterResponse](
					func(hc actor.HandlerCtx, _ GetValue) (*CounterResponse, error) {
						hc.Log().Info("get value", slog.Int("value", value))
						return &CounterResponse{CounterID: counterID, Value: value}, nil
					},
				),
			),
		), nil
	})
}

// =============================================================================
// Infrastructure
// =============================================================================

func startNATSContainer(ctx context.Context, log *slog.Logger) (string, func(), error) {
	natsC, err := testcontainers.Run(
		ctx, "nats:latest",
		testcontainers.WithCmd("-js"),
		testcontainers.WithExposedPorts("4222/tcp"),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("4222/tcp"),
			wait.ForLog("Server is ready"),
		),
	)
	if err != nil {
		return "", nil, fmt.Errorf("start container: %w", err)
	}

	ip, err := natsC.ContainerIP(ctx)
	if err != nil {
		_ = testcontainers.TerminateContainer(natsC)
		return "", nil, fmt.Errorf("get container IP: %w", err)
	}

	natsURL := fmt.Sprintf("nats://%s:4222", ip)
	cleanup := func() {
		log.Info("terminating NATS container...")
		if err := testcontainers.TerminateContainer(natsC); err != nil {
			log.Error("failed to terminate container", slog.Any("error", err))
		}
	}

	return natsURL, cleanup, nil
}
