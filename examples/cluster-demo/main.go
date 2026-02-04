// Package main demonstrates a distributed actor cluster using NATS JetStream.
//
// This example shows:
//   - Spinning up NATS via testcontainers
//   - Creating a multi-node cluster with shard distribution
//   - Actors handling typed request/response messages
//   - Client routing requests to the correct shard/node
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/codewandler/clstr-go/adapters/nats"
	"github.com/codewandler/clstr-go/core/actor/v2"
	"github.com/codewandler/clstr-go/core/cluster"
)

// Message types for the demo
type (
	// GreetRequest asks an actor to greet someone.
	GreetRequest struct {
		Name string `json:"name"`
	}
	// GreetResponse is the greeting reply.
	GreetResponse struct {
		Message string `json:"message"`
	}

	// CountRequest increments a counter and returns the new value.
	CountRequest struct{}
	// CountResponse returns the current count.
	CountResponse struct {
		Count int `json:"count"`
	}
)

const (
	numNodes  = 3
	numShards = 64
	seed      = "demo-cluster"
)

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
	// Start NATS container
	log.Info("starting NATS container...")
	natsURL, cleanup, err := startNATSContainer(ctx, log)
	if err != nil {
		return fmt.Errorf("start nats: %w", err)
	}
	defer cleanup()
	log.Info("NATS ready", slog.String("url", natsURL))

	// Create transports for each node (each node needs its own transport connection)
	transports := make([]*nats.Transport, numNodes)
	for i := range numNodes {
		tr, err := nats.NewTransport(nats.TransportConfig{
			Connect:       nats.ConnectURL(natsURL),
			Log:           log.With(slog.Int("node", i)),
			SubjectPrefix: "demo",
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
			Handler:   createActorHandler(nodeCtx, log.With(slog.String("node", nodeID))),
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

	// Create a client
	client, err := cluster.NewClient(cluster.ClientOptions{
		Transport: transports[0], // Reuse first node's transport for client
		NumShards: numShards,
		Seed:      seed,
	})
	if err != nil {
		return fmt.Errorf("create client: %w", err)
	}

	// Demo: send requests to different keys (will be routed to different actors/shards)
	log.Info("sending demo requests...")

	keys := []string{"alice", "bob", "charlie", "alice", "bob"}
	for _, key := range keys {
		scopedClient := client.Key(key)

		// Send greet request
		greetRes, err := cluster.NewRequest[GreetRequest, GreetResponse](scopedClient).
			Request(ctx, GreetRequest{Name: key})
		if err != nil {
			return fmt.Errorf("greet %s: %w", key, err)
		}
		log.Info("greet response",
			slog.String("key", key),
			slog.String("message", greetRes.Message),
		)

		// Send count request
		countRes, err := cluster.NewRequest[CountRequest, CountResponse](scopedClient).
			Request(ctx, CountRequest{})
		if err != nil {
			return fmt.Errorf("count %s: %w", key, err)
		}
		log.Info("count response",
			slog.String("key", key),
			slog.Int("count", countRes.Count),
		)
	}

	// Demonstrate that actors maintain state per key
	log.Info("verifying per-key state...")
	for _, key := range []string{"alice", "bob", "charlie"} {
		countRes, err := cluster.NewRequest[CountRequest, CountResponse](client.Key(key)).
			Request(ctx, CountRequest{})
		if err != nil {
			return fmt.Errorf("final count %s: %w", key, err)
		}
		log.Info("final count",
			slog.String("key", key),
			slog.Int("count", countRes.Count),
		)
	}

	log.Info("demo complete, shutting down...")
	nodeCancel()
	wg.Wait()

	return nil
}

// createActorHandler returns a cluster handler that spawns one actor per key.
// Each actor maintains its own state (counter).
func createActorHandler(ctx context.Context, log *slog.Logger) cluster.ServerHandlerFunc {
	return cluster.NewActorHandler(func(key string) (actor.Actor, error) {
		// Each key gets its own actor with its own state
		counter := 0

		return actor.New(
			actor.Options{
				Context:     ctx,
				Logger:      log.With(slog.String("key", key)),
				MailboxSize: 256,
			},
			actor.TypedHandlers(
				actor.Init(func(hc actor.HandlerCtx) error {
					hc.Log().Info("actor initialized")
					return nil
				}),

				actor.HandleRequest[GreetRequest, GreetResponse](
					func(hc actor.HandlerCtx, req GreetRequest) (*GreetResponse, error) {
						hc.Log().Debug("handling greet", slog.String("name", req.Name))
						return &GreetResponse{
							Message: fmt.Sprintf("Hello, %s! (from actor for key '%s')", req.Name, key),
						}, nil
					},
				),

				actor.HandleRequest[CountRequest, CountResponse](
					func(hc actor.HandlerCtx, _ CountRequest) (*CountResponse, error) {
						counter++
						hc.Log().Debug("handling count", slog.Int("count", counter))
						return &CountResponse{Count: counter}, nil
					},
				),
			),
		), nil
	})
}

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
