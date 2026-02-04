// Package main is a performance benchmark for the Event Sourcing pillar.
//
// This example measures throughput and latency of aggregate load/save operations
// with configurable backends, snapshots, and caching strategies.
//
// Run with: go run .
//
// Configure via environment variables:
//
//	N=50000          Total number of operations
//	U=100            Number of unique users (aggregate keys)
//	B=1000           Batch size for progress reporting
//	BACKEND=nats     Backend type: "nats" or "mem"
//	SNAPSHOT=true    Enable snapshot optimization
//	CACHE=false      Enable LRU cache
//	LOAD_AFTER_SAVE=false  Load aggregate after each save
package main

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/codewandler/clstr-go/adapters/nats"
	"github.com/codewandler/clstr-go/core/cache"
	"github.com/codewandler/clstr-go/core/es"
)

// =============================================================================
// Configuration
// =============================================================================

var (
	totalOps      = getEnvInt("N", 50_000)
	numUsers      = getEnvInt("U", 100)
	batchSize     = getEnvInt("B", 1_000)
	backendType   = getEnv("BACKEND", "nats")
	useSnapshot   = getEnvBool("SNAPSHOT", true)
	useCache      = getEnvBool("CACHE", false)
	loadAfterSave = getEnvBool("LOAD_AFTER_SAVE", false)
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
		log.Error("loadtest failed", slog.Any("error", err))
		os.Exit(1)
	}
}

func run(ctx context.Context, log *slog.Logger) error {
	// Print configuration
	fmt.Println("=== Load Test Configuration ===")
	fmt.Printf("  Backend:         %s\n", backendType)
	fmt.Printf("  Total ops:       %d\n", totalOps)
	fmt.Printf("  Unique users:    %d\n", numUsers)
	fmt.Printf("  Batch size:      %d\n", batchSize)
	fmt.Printf("  Snapshots:       %v\n", useSnapshot)
	fmt.Printf("  Cache:           %v\n", useCache)
	fmt.Printf("  Load after save: %v\n", loadAfterSave)
	fmt.Println()

	// Create environment based on backend type
	var env *es.Env
	var cleanup func()

	switch backendType {
	case "nats":
		log.Info("starting NATS container...")
		natsURL, natsCleanup, err := startNATSContainer(ctx, log)
		if err != nil {
			return fmt.Errorf("start nats: %w", err)
		}
		cleanup = natsCleanup
		log.Info("NATS ready", slog.String("url", natsURL))

		env, err = createNatsEnv(ctx, log, natsURL)
		if err != nil {
			cleanup()
			return fmt.Errorf("create nats env: %w", err)
		}
	default:
		log.Info("using in-memory backend")
		env = createMemEnv(ctx, log)
	}

	if cleanup != nil {
		defer cleanup()
	}
	defer env.Shutdown()

	// Create repository with optional caching
	var cacheOption es.RepositoryOption
	if useCache {
		cacheOption = es.WithRepoCacheLRU(1_000)
	} else {
		cacheOption = es.WithRepoCache(cache.NewNop())
	}

	repo := es.NewTypedRepositoryFrom[*User](log, env.Repository(), cacheOption)

	// Run the benchmark
	log.Info("=== Starting Load Test ===")
	return runBenchmark(ctx, log, repo)
}

// =============================================================================
// Benchmark
// =============================================================================

func runBenchmark(ctx context.Context, log *slog.Logger, repo es.TypedRepository[*User]) error {
	startAt := time.Now()
	lastTime := time.Now()

	for i := range totalOps {
		select {
		case <-ctx.Done():
			log.Info("interrupted")
			return ctx.Err()
		default:
		}

		userID := fmt.Sprintf("user-%d", rand.Intn(numUsers))

		// Load or create user
		user, err := repo.GetOrCreate(ctx, userID, es.WithSnapshot(useSnapshot))
		if err != nil {
			return fmt.Errorf("get user %s: %w", userID, err)
		}

		// Make a change
		if err := user.ChangeEmail(fmt.Sprintf("%s@host-%d.example.com", userID, i)); err != nil {
			return fmt.Errorf("change email: %w", err)
		}

		// Save
		if err := repo.Save(ctx, user, es.WithSnapshot(useSnapshot)); err != nil {
			return fmt.Errorf("save user %s: %w", userID, err)
		}

		// Optional: verify by loading again
		if loadAfterSave {
			if _, err := repo.GetByID(ctx, userID, es.WithSnapshot(useSnapshot)); err != nil {
				return fmt.Errorf("reload user %s: %w", userID, err)
			}
		}

		// Progress reporting
		if i > 0 && i%100 == 0 {
			fmt.Print(".")
		}
		if i > 0 && i%batchSize == 0 {
			mem := getMemUsage()
			elapsed := time.Since(lastTime)
			rate := float64(batchSize) / elapsed.Seconds()
			fmt.Printf(" | %5d ops | %6d ms | %6.0f ops/s | %d/%d MiB (heap/sys)\n",
				batchSize, elapsed.Milliseconds(), rate,
				mem.Alloc/1024/1024, mem.Sys/1024/1024)
			lastTime = time.Now()
		}
	}

	// Final stats
	fmt.Println()
	fmt.Println("=== Results ===")
	runtime.GC()

	totalTime := time.Since(startAt)
	avgRate := float64(totalOps) / totalTime.Seconds()

	fmt.Printf("  Total time:    %.3f seconds\n", totalTime.Seconds())
	fmt.Printf("  Total ops:     %d\n", totalOps)
	fmt.Printf("  Avg rate:      %.0f ops/s\n", avgRate)

	mem := getMemUsage()
	fmt.Printf("  Final memory:  %d MiB heap, %d MiB sys\n", mem.Alloc/1024/1024, mem.Sys/1024/1024)

	return nil
}

// =============================================================================
// Domain: User Aggregate
// =============================================================================

type User struct {
	es.BaseAggregate
	Name  string
	Email string
}

func (u *User) GetAggType() string { return "user" }

func (u *User) Register(r es.Registrar) {
	es.RegisterEvents(r,
		es.Event[NameChanged](),
		es.Event[EmailChanged](),
	)
}

func (u *User) Apply(e any) error {
	switch evt := e.(type) {
	case *es.AggregateCreatedEvent:
		return u.BaseAggregate.Apply(evt)
	case *NameChanged:
		u.Name = evt.NewName
	case *EmailChanged:
		u.Email = evt.NewEmail
	}
	return nil
}

func (u *User) ChangeName(name string) error {
	if name == "" {
		return fmt.Errorf("name is empty")
	}
	return es.RaiseAndApply(u, &NameChanged{NewName: name})
}

func (u *User) ChangeEmail(email string) error {
	if email == "" {
		return fmt.Errorf("email is empty")
	}
	return es.RaiseAndApply(u, &EmailChanged{NewEmail: email})
}

// Events
type (
	NameChanged  struct{ NewName string }
	EmailChanged struct{ NewEmail string }
)

// =============================================================================
// Environment Setup
// =============================================================================

func createMemEnv(ctx context.Context, log *slog.Logger) *es.Env {
	return es.NewEnv(
		es.WithCtx(ctx),
		es.WithLog(log),
		es.WithInMemory(),
		es.WithAggregates(new(User)),
	)
}

func createNatsEnv(ctx context.Context, log *slog.Logger, natsURL string) (*es.Env, error) {
	connect := nats.ConnectURL(natsURL)

	store, err := nats.NewEventStore(nats.EventStoreConfig{
		Log:            log,
		Connect:        connect,
		SubjectPrefix:  "loadtest.es",
		StreamName:     "LOADTEST_EVENTS",
		StreamSubjects: []string{"loadtest.>"},
		MaxMsgs:        1_000_000,
	})
	if err != nil {
		return nil, fmt.Errorf("create event store: %w", err)
	}

	snapshotter, err := nats.NewSnapshotter(nats.KvConfig{
		Connect: connect,
		TTL:     5 * time.Minute,
		Bucket:  "loadtest_snapshots",
	})
	if err != nil {
		return nil, fmt.Errorf("create snapshotter: %w", err)
	}

	return es.NewEnv(
		es.WithCtx(ctx),
		es.WithLog(log),
		es.WithStore(store),
		es.WithSnapshotter(snapshotter),
		es.WithAggregates(new(User)),
	), nil
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

// =============================================================================
// Helpers
// =============================================================================

type memUsage struct {
	Alloc uint64
	Sys   uint64
}

func getMemUsage() memUsage {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return memUsage{Alloc: m.Alloc, Sys: m.Sys}
}

func getEnv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	v, err := strconv.Atoi(getEnv(key, fmt.Sprintf("%d", fallback)))
	if err != nil {
		return fallback
	}
	return v
}

func getEnvBool(key string, fallback bool) bool {
	v := getEnv(key, "")
	if v == "" {
		return fallback
	}
	return v == "1" || strings.EqualFold(v, "true")
}
