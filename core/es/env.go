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
	ctx         context.Context
	log         *slog.Logger
	snapshotter Snapshotter
	cpStore     CpStore
	subCpStore  SubCpStore
	store       EventStore
	events      []EventRegisterOption
	projections []Projection
	aggregates  []Aggregate
}

func newEnvOptions(opts ...EnvOption) envOptions {
	options := envOptions{
		ctx:        context.Background(),
		store:      NewInMemoryStore(),
		cpStore:    NewInMemoryCpStore(),
		subCpStore: NewInMemorySubCpStore(),
	}
	for _, opt := range opts {
		opt.applyToEnv(&options)
	}
	return options
}

type Env struct {
	ctx         context.Context
	log         *slog.Logger
	store       EventStore
	cpStore     CpStore
	subCpStore  SubCpStore
	snapshotter Snapshotter
	registry    *EventRegistry
	pRunner     *ProjectionRunner
	repo        Repository
}

func (e *Env) Repository() Repository   { return e.repo }
func (e *Env) Store() EventStore        { return e.store }
func (e *Env) Snapshotter() Snapshotter { return e.snapshotter }

func NewEnv(opts ...EnvOption) (e *Env, err error) {
	options := newEnvOptions(opts...)
	e = &Env{
		ctx:         options.ctx,
		log:         options.log,
		store:       options.store,
		cpStore:     options.cpStore,
		subCpStore:  options.subCpStore,
		snapshotter: options.snapshotter,
		registry:    NewRegistry(),
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
	e.pRunner = NewProjectionRunner(
		e.log,
		e.registry,
		e.subCpStore,
		e.cpStore,
	)
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

	err = e.startProjections()
	if err != nil {
		return nil, fmt.Errorf("failed to start projections: %w", err)
	}

	return e, nil
}

func (e *Env) startProjections() error {
	e.log.Debug("env running...")

	done := make(chan struct{})

	var startSeq uint64
	var err error
	startSeq, err = e.pRunner.GetStartSeq()
	if err != nil {
		return fmt.Errorf("failed to get subscription start sequence")
	}

	sub, err := e.store.Subscribe(e.ctx, WithDeliverPolicy(DeliverAllPolicy), WithStartSequence(startSeq))
	if err != nil {
		e.log.Error("failed to subscribe to events", "err", err)
	}

	go func(sub Subscription) {
		defer sub.Cancel()

		hdl := e.pRunner.Handler()

		done <- struct{}{}

		for {
			select {
			case <-e.ctx.Done():
				return
			case ev := <-sub.Chan():
				err := hdl(e.ctx, []Envelope{ev})
				if err != nil {
					e.log.Error("projection handler failed", "err", err)
				}
			}
		}
	}(sub)

	<-done

	return nil

}
