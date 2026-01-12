package es

import (
	"context"
	"fmt"
	"log/slog"
)

type EnvOption interface {
	applyToEnv(*envOptions)
}

type envOptions struct {
	ctx             context.Context
	log             *slog.Logger
	snapshotter     Snapshotter
	checkpointStore CheckpointStore
	store           EventStore
	events          []EventRegisterOption
	projections     []Projection
	aggregates      []Aggregate
}

func newEnvOptions(opts ...EnvOption) envOptions {
	options := envOptions{
		ctx:             context.Background(),
		store:           NewInMemoryStore(),
		checkpointStore: NewInMemoryCheckpointStore(),
	}
	for _, opt := range opts {
		opt.applyToEnv(&options)
	}
	return options
}

type Env struct {
	ctx             context.Context
	log             *slog.Logger
	store           EventStore
	checkpointStore CheckpointStore
	snapshotter     Snapshotter
	registry        *EventRegistry
	pRunner         *ProjectionRunner
	repo            Repository
}

func (e *Env) Repository() Repository   { return e.repo }
func (e *Env) Store() EventStore        { return e.store }
func (e *Env) Snapshotter() Snapshotter { return e.snapshotter }

func NewEnv(opts ...EnvOption) *Env {
	options := newEnvOptions(opts...)
	e := &Env{
		ctx:             options.ctx,
		log:             options.log,
		store:           options.store,
		checkpointStore: options.checkpointStore,
		snapshotter:     options.snapshotter,
		registry:        NewRegistry(),
	}

	if e.log == nil {
		e.log = slog.Default()
	}

	if e.ctx == nil {
		e.ctx = context.Background()
	}

	for _, agg := range options.aggregates {
		agg.Register(e.registry)
		e.log.Debug("registered aggregate", "type", fmt.Sprintf("%T", agg))
	}

	// register events
	RegisterEventFor[AggregateCreatedEvent](e.registry)
	for _, s := range options.events {
		e.registry.Register(s.t, s.ctor)
		e.log.Debug("registered event", "type", s.t)
	}

	// register projections
	e.pRunner = NewProjectionRunner(e.log, e.registry, e.checkpointStore)
	for _, p := range options.projections {
		e.pRunner.Register(p)
	}

	// create repository
	e.repo = NewRepository(
		e.log,
		e.store,
		e.registry,
		WithSnapshotter(e.snapshotter),
	)

	e.startProjections(e.ctx)

	return e
}

func (e *Env) startProjections(ctx context.Context) {
	e.log.Debug("env running...")

	done := make(chan struct{})

	go func() {
		sub, err := e.store.Subscribe(ctx, WithDeliverPolicy(DeliverAllPolicy))
		if err != nil {
			e.log.Error("failed to subscribe to events", "err", err)
		}
		defer sub.Cancel()

		hdl := e.pRunner.Handler()

		done <- struct{}{}

		for {
			select {
			case <-ctx.Done():
				return
			case ev := <-sub.Chan():
				err := hdl(ctx, []Envelope{ev})
				if err != nil {
					e.log.Error("projection handler failed", "err", err)
				}
			}
		}
	}()

	<-done

}
