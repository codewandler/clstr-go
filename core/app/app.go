package app

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	gonanoid "github.com/matoous/go-nanoid/v2"

	"github.com/codewandler/clstr-go/core/actor/v2"
	"github.com/codewandler/clstr-go/core/cluster"
)

// NodeConfig configures the cluster node.
type NodeConfig struct {
	// ID is the node identifier. Auto-generated if empty.
	ID string
	// NumShards is the total number of shards (default: 256).
	NumShards uint32
	// NodeIDs lists all node IDs in the cluster. Defaults to [ID] for single-node.
	NodeIDs []string
	// ShardSeed is used for deterministic shard distribution (default: "default").
	ShardSeed string
	// Transport is the communication layer. Defaults to in-memory for testing.
	Transport cluster.Transport
	// Metrics for cluster instrumentation. Optional.
	Metrics cluster.ClusterMetrics
}

// ActorOptions configures actor creation.
type ActorOptions struct {
	// MailboxSize is the actor mailbox capacity (default: 256).
	MailboxSize int
	// MaxConcurrentTasks caps scheduled tasks via HandlerCtx.Schedule.
	// If 0 or negative, scheduling is unlimited.
	MaxConcurrentTasks int
	// Metrics for actor instrumentation. Optional.
	Metrics actor.ActorMetrics
}

// Config is the application configuration.
type Config struct {
	// Context is the parent context for cancellation. Defaults to context.Background().
	Context context.Context
	// Log is the logger instance. Defaults to slog.Default().
	Log *slog.Logger
	// Node configures the cluster node.
	Node NodeConfig
	// Actor configures actor creation options.
	Actor ActorOptions
	// CreateActor overrides the default actor factory. If set, Actor options are ignored.
	CreateActor func(key string) (actor.Actor, error)
}

// App combines a cluster Node and Client with actor-based message handling.
type App struct {
	ctx       context.Context
	log       *slog.Logger
	cancelCtx context.CancelFunc
	node      *cluster.Node
	client    *cluster.Client

	runOnce  sync.Once
	stopOnce sync.Once
	done     chan struct{}
}

// New creates a new App without starting it. Call Run() to start.
func New(config Config, handlers ...actor.HandlerRegistration) (app *App, err error) {
	app = &App{
		done: make(chan struct{}),
	}

	// === node config ===
	nodeConfig := config.Node
	if nodeConfig.ID == "" {
		nodeConfig.ID = fmt.Sprintf("node-%s", gonanoid.Must(6))
	}
	if len(nodeConfig.NodeIDs) == 0 {
		nodeConfig.NodeIDs = []string{nodeConfig.ID}
	}
	if nodeConfig.NumShards == 0 {
		nodeConfig.NumShards = 256
	}
	if nodeConfig.ShardSeed == "" {
		nodeConfig.ShardSeed = "default"
	}
	if nodeConfig.Transport == nil {
		nodeConfig.Transport = cluster.NewInMemoryTransport()
	}

	// === logger ===
	if config.Log == nil {
		config.Log = slog.Default()
	}
	app.log = config.Log.With(slog.String("node", nodeConfig.ID))

	// === context ===
	if config.Context == nil {
		config.Context = context.Background()
	}
	app.ctx, app.cancelCtx = context.WithCancel(config.Context)

	// === actor options ===
	actorOpts := config.Actor
	if actorOpts.MailboxSize == 0 {
		actorOpts.MailboxSize = 256
	}

	// === actor factory ===
	createActor := config.CreateActor
	if createActor == nil {
		createActor = func(key string) (actor.Actor, error) {
			actLog := app.log.With(slog.String("actor", key))
			return actor.New(
				actor.Options{
					Context:            app.ctx,
					Logger:             actLog,
					MailboxSize:        actorOpts.MailboxSize,
					MaxConcurrentTasks: actorOpts.MaxConcurrentTasks,
					Metrics:            actorOpts.Metrics,
					OnPanic: func(recovered any, stack []byte, msg any) {
						actLog.Error("actor panicked", slog.Any("recovered", recovered), slog.Any("stack", stack), slog.Any("msg", msg))
					},
				},
				actor.TypedHandlers(func(registrar actor.HandlerRegistrar) {
					for _, h := range handlers {
						h(registrar)
					}
				}),
			), nil
		}
	}

	app.log.Debug("creating app", slog.Any("node_config", nodeConfig))

	app.node = cluster.NewNode(cluster.NodeOptions{
		NodeID:    nodeConfig.ID,
		Shards:    cluster.ShardsForNode(nodeConfig.ID, nodeConfig.NodeIDs, nodeConfig.NumShards, nodeConfig.ShardSeed),
		Log:       app.log,
		Transport: nodeConfig.Transport,
		Handler:   cluster.NewActorHandler(createActor),
		Metrics:   nodeConfig.Metrics,
	})

	// === create client ===
	app.client, err = cluster.NewClient(cluster.ClientOptions{
		NumShards: nodeConfig.NumShards,
		Seed:      nodeConfig.ShardSeed,
		Transport: nodeConfig.Transport,
		Metrics:   nodeConfig.Metrics,
	})
	if err != nil {
		return nil, err
	}

	return app, nil
}

// Client returns the cluster client for sending requests.
func (a *App) Client() *cluster.Client { return a.client }

// Node returns the underlying cluster node.
func (a *App) Node() *cluster.Node { return a.node }

// Done returns a channel that is closed when the app has fully stopped.
func (a *App) Done() <-chan struct{} { return a.done }

// Run starts the cluster node. Blocks until the context is cancelled.
// Can only be called once.
func (a *App) Run() error {
	var runErr error
	a.runOnce.Do(func() {
		runErr = a.node.Run(a.ctx)
		if runErr != nil {
			close(a.done)
			return
		}
		a.log.Info("app started")
	})
	return runErr
}

// Stop initiates graceful shutdown by cancelling the context.
// Non-blocking; use Done() or Shutdown() to wait for completion.
func (a *App) Stop() {
	a.stopOnce.Do(func() {
		a.cancelCtx()
		close(a.done)
	})
}

// Shutdown stops the app and waits for it to finish or the context to expire.
// Returns ctx.Err() if the context expires before shutdown completes.
func (a *App) Shutdown(ctx context.Context) error {
	a.Stop()
	select {
	case <-a.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Run creates and starts an App. Convenience function combining New() and Run().
func Run(config Config, handlers ...actor.HandlerRegistration) (*App, error) {
	app, err := New(config, handlers...)
	if err != nil {
		return nil, err
	}

	if err := app.Run(); err != nil {
		return nil, err
	}

	return app, nil
}
