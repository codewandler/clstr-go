package es

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/codewandler/clstr-go/internal/reflector"
	gonanoid "github.com/matoous/go-nanoid/v2"
)

type Env struct {
	ctx          context.Context
	id           string
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

func NewEnv(opts ...EnvOption) (e *Env, err error) {
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

	// start all consumers
	for _, c := range options.consumers {
		consumer := e.NewConsumer(c.handler, WithConsumerOpts(WithLog(e.log)), WithConsumerOpts(c.consumerOpts...))
		if err := consumer.Start(e.ctx); err != nil {
			return nil, fmt.Errorf("failed to start consumer: %w", err)
		}
		e.consumers = append(e.consumers, consumer)
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

	return e, nil
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

func (e *Env) Append(ctx context.Context, expect Version, aggType string, aggID string, events ...any) error {
	_, err := e.AppendWithResult(ctx, expect, aggType, aggID, events...)
	return err
}

func (e *Env) AppendWithResult(
	ctx context.Context,
	expect Version,
	aggType string,
	aggID string,
	events ...any,
) (*StoreAppendResult, error) {
	envelopes := make([]Envelope, 0)
	for i, ev := range events {
		data, err := json.Marshal(ev)
		if err != nil {
			return nil, err
		}
		envelopes = append(envelopes, Envelope{
			ID:            gonanoid.Must(),
			Type:          reflector.TypeInfoOf(ev).Name,
			AggregateID:   aggID,
			AggregateType: aggType,
			Data:          data,
			OccurredAt:    time.Now(),
			Version:       expect + Version(i+1),
		})
	}
	return e.store.Append(
		ctx,
		aggType,
		aggID,
		expect,
		envelopes,
	)
}
