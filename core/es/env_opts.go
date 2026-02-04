package es

import (
	"context"
	"fmt"
	"log/slog"
)

type (
	envOptions struct {
		ctx         context.Context
		log         *slog.Logger
		snapshotter Snapshotter
		store       EventStore
		events      []EventRegisterOption
		aggregates  []Aggregate
		consumers   []EnvConsumerOption
		metrics     ESMetrics
	}

	EnvOption interface {
		applyToEnv(*envOptions)
	}
)

func newEnvOptions(opts ...EnvOption) envOptions {
	options := envOptions{
		ctx:   context.Background(),
		store: NewInMemoryStore(),
	}
	for _, opt := range opts {
		opt.applyToEnv(&options)
	}
	return options
}

// === options ===

type (
	EnvConsumerOption struct {
		handler      Handler
		consumerOpts []ConsumerOption
	}
)

func WithConsumer(handler Handler, opts ...ConsumerOption) EnvConsumerOption {
	return EnvConsumerOption{
		handler:      handler,
		consumerOpts: opts,
	}
}

func WithProjection(projection Projection, opts ...ConsumerOption) EnvConsumerOption {
	return EnvConsumerOption{
		handler:      projection,
		consumerOpts: append(opts, WithConsumerName(fmt.Sprintf("projection/%s", projection.Name()))),
	}
}

func (o EnvConsumerOption) applyToEnv(options *envOptions) {
	options.consumers = append(options.consumers, o)
}
