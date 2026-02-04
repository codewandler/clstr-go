package es

import (
	"fmt"
	"log/slog"
	"time"

	gonanoid "github.com/matoous/go-nanoid/v2"
)

type (
	consumerOpts struct {
		startSeq        uint64
		mws             []HandlerMiddleware
		log             *slog.Logger
		name            string
		shutdownTimeout time.Duration
		metrics         ESMetrics
	}

	ConsumerOption interface {
		applyToConsumerOpts(*consumerOpts)
	}

	ConsumerNameOption            valueOption[string]
	MiddlewareOption              valueOption[[]HandlerMiddleware]
	ConsumerOptions               MultiOption[ConsumerOption]
	ConsumerShutdownTimeoutOption valueOption[time.Duration]
)

func (o ConsumerNameOption) applyToConsumerOpts(opts *consumerOpts) { opts.name = o.v }
func (o StartSeqOption) applyToConsumerOpts(opts *consumerOpts)     { opts.startSeq = o.v }
func (o ConsumerShutdownTimeoutOption) applyToConsumerOpts(opts *consumerOpts) {
	opts.shutdownTimeout = o.v
}
func (o MiddlewareOption) applyToConsumerOpts(opts *consumerOpts) {
	opts.mws = append(opts.mws, o.v...)
}
func (o LogOption) applyToConsumerOpts(opts *consumerOpts) { opts.log = o.l }
func (o ConsumerOptions) applyToConsumerOpts(opts *consumerOpts) {
	for _, opt := range o.opts {
		opt.applyToConsumerOpts(opts)
	}
}

func WithMiddlewares(mws ...HandlerMiddleware) MiddlewareOption {
	return MiddlewareOption{
		v: mws,
	}
}
func WithMiddlewaresAppend(mws ...HandlerMiddleware) MiddlewareOption {
	return MiddlewareOption{
		v: append(mws, mws...),
	}
}
func WithConsumerOpts(opts ...ConsumerOption) ConsumerOptions { return ConsumerOptions{opts: opts} }
func WithConsumerName(name string) ConsumerNameOption         { return ConsumerNameOption{name} }

// WithShutdownTimeout sets the timeout for handler shutdown when the consumer stops.
// Default is 5 seconds.
func WithShutdownTimeout(d time.Duration) ConsumerShutdownTimeoutOption {
	return ConsumerShutdownTimeoutOption{v: d}
}

func newConsumerOpts(opts ...ConsumerOption) consumerOpts {
	options := consumerOpts{
		log:             slog.Default(),
		startSeq:        1,
		name:            fmt.Sprintf("consumer-%s", gonanoid.Must(6)),
		shutdownTimeout: 5 * time.Second,
	}
	for _, opt := range opts {
		opt.applyToConsumerOpts(&options)
	}
	return options
}
