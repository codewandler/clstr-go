package app

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/codewandler/clstr-go/core/actor/v2"
	"github.com/codewandler/clstr-go/core/cluster"
	gonanoid "github.com/matoous/go-nanoid/v2"
)

type NodeConfig struct {
	ID        string
	NumShards uint32
	NodeIDs   []string
	ShardSeed string
	Transport cluster.Transport
}

type ActorConfig struct {
	RegisterHandlers actor.HandlerRegistration
}

type Config struct {
	Context context.Context
	Log     *slog.Logger
	Node    NodeConfig
	Actor   func(key string) (actor.Actor, error)
}

type App struct {
	ctx       context.Context
	log       *slog.Logger
	cancelCtx context.CancelFunc
	node      *cluster.Node
	client    *cluster.Client
}

func New(config Config, handlers ...actor.HandlerRegistration) (app *App, err error) {
	app = &App{}

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

	// === actor config ===
	createActor := config.Actor
	if createActor == nil {
		createActor = func(key string) (actor.Actor, error) {
			actLog := app.log.With(slog.String("actor", key))
			return actor.New(
				actor.Options{
					Context: app.ctx,
					Logger:  actLog,
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
	})

	// === create client ===

	app.client, err = cluster.NewClient(cluster.ClientOptions{
		NumShards: nodeConfig.NumShards,
		Seed:      nodeConfig.ShardSeed,
		Transport: nodeConfig.Transport,
	})
	if err != nil {
		return nil, err
	}

	return app, nil
}

func (a *App) Client() *cluster.Client { return a.client }

func (a *App) Run() (err error) {
	err = a.node.Run(a.ctx)
	if err != nil {
		return err
	}

	a.log.Info("app started")

	return nil
}

func (a *App) Stop() {
	a.cancelCtx()
}

func Run(config Config, handlers ...actor.HandlerRegistration) (app *App, err error) {
	app, err = New(config, handlers...)
	if err != nil {
		return nil, err
	}

	err = app.Run()
	if err != nil {
		return nil, err
	}

	return app, nil
}
