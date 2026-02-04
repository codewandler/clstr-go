package es

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	gonanoid "github.com/matoous/go-nanoid/v2"
)

type Env struct {
	ctx          context.Context
	done         chan struct{}
	shutdownOnce sync.Once
	cancelCtx    context.CancelFunc
	log          *slog.Logger
	store        EventStore
	snapshotter  Snapshotter
	registry     *EventRegistry
	repo         Repository
	consumers    []*Consumer
}

func (e *Env) Repository() Repository   { return e.repo }
func (e *Env) Store() EventStore        { return e.store }
func (e *Env) Snapshotter() Snapshotter { return e.snapshotter }

func NewEnv(opts ...EnvOption) (e *Env) {
	var (
		id      = gonanoid.Must(6)
		options = newEnvOptions(opts...)
	)

	// ctx
	ctx := options.ctx
	if ctx == nil {
		ctx = context.Background()
	}

	// log
	log := options.log
	if log == nil {
		log = slog.Default()
	}
	log = log.With(slog.String("env", id))

	e = &Env{
		log:         log,
		store:       options.store,
		snapshotter: options.snapshotter,
		registry:    NewRegistry(),
		done:        make(chan struct{}),
		consumers:   make([]*Consumer, 0),
	}
	e.ctx, e.cancelCtx = context.WithCancel(ctx)

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

	// create repository
	e.repo = NewRepository(
		e.log,
		e.store,
		e.registry,
		WithSnapshotter(e.snapshotter),
	)

	// build consumers
	for _, c := range options.consumers {
		e.consumers = append(e.consumers, e.NewConsumer(c.handler, WithConsumerOpts(WithLog(e.log)), WithConsumerOpts(c.consumerOpts...)))
	}

	context.AfterFunc(e.ctx, func() {
		e.log.Info("shutting down")

		e.log.Debug("stopping consumers", slog.Int("count", len(e.consumers)))
		for _, c := range e.consumers {
			c.Stop()
		}

		// we are done
		e.log.Info("env shutdown")
		close(e.done)
	})

	return e
}

func (e *Env) Start() (err error) {
	// start all consumers
	for _, c := range e.consumers {
		if err := c.Start(e.ctx); err != nil {
			return fmt.Errorf("failed to start consumer: %w", err)
		}
	}

	return nil
}

func (e *Env) Shutdown() {
	e.shutdownOnce.Do(func() {
		e.cancelCtx()
		<-e.done
	})
}

func (e *Env) NewConsumer(handler Handler, opts ...ConsumerOption) *Consumer {
	return NewConsumer(e.store, e.registry, handler, WithLog(e.log), WithConsumerOpts(opts...))
}

func (e *Env) Append(ctx context.Context, aggType string, aggID string, expect Version, events ...any) error {
	_, err := AppendEvents(ctx, e.store, aggType, aggID, expect, events...)
	return err
}
