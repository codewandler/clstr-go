package es

import (
	"fmt"
	"log/slog"
	"time"

	gonanoid "github.com/matoous/go-nanoid/v2"
)

type (
	consumerOpts struct {
		startSeq                uint64
		mws                     []HandlerMiddleware
		log                     *slog.Logger
		name                    string
		shutdownTimeout         time.Duration
		reconnectBackoffInitial time.Duration
		reconnectBackoffMax     time.Duration
		metrics                 ESMetrics
	}

	ConsumerOption interface {
		applyToConsumerOpts(*consumerOpts)
	}

	ConsumerNameOption            valueOption[string]
	MiddlewareOption              valueOption[[]HandlerMiddleware]
	SetMiddlewareOption           valueOption[[]HandlerMiddleware]
	ConsumerOptions               MultiOption[ConsumerOption]
	ConsumerShutdownTimeoutOption valueOption[time.Duration]
)

func (o ConsumerNameOption) applyToConsumerOpts(opts *consumerOpts) { opts.name = o.v }
func (o StartSeqOption) applyToConsumerOpts(opts *consumerOpts)     { opts.startSeq = o.v }
func (o ConsumerShutdownTimeoutOption) applyToConsumerOpts(opts *consumerOpts) {
	opts.shutdownTimeout = o.v
}
// MiddlewareOption appends middlewares to the existing list.
func (o MiddlewareOption) applyToConsumerOpts(opts *consumerOpts) {
	opts.mws = append(opts.mws, o.v...)
}
// SetMiddlewareOption replaces the middleware list entirely.
func (o SetMiddlewareOption) applyToConsumerOpts(opts *consumerOpts) {
	opts.mws = o.v
}
func (o LogOption) applyToConsumerOpts(opts *consumerOpts) { opts.log = o.l }
func (o ConsumerOptions) applyToConsumerOpts(opts *consumerOpts) {
	for _, opt := range o.opts {
		opt.applyToConsumerOpts(opts)
	}
}

// WithMiddlewares sets the complete list of handler middlewares, replacing
// any previously configured middlewares. Middlewares are applied in the
// order they are provided (first = outermost wrapper).
// Use WithMiddlewaresAppend to extend an existing list instead.
func WithMiddlewares(mws ...HandlerMiddleware) SetMiddlewareOption {
	return SetMiddlewareOption{v: mws}
}

// WithMiddlewaresAppend appends the given middlewares to any previously
// configured middlewares without discarding the existing ones.
// Use WithMiddlewares to replace the entire list instead.
func WithMiddlewaresAppend(mws ...HandlerMiddleware) MiddlewareOption {
	return MiddlewareOption{v: mws}
}
func WithConsumerOpts(opts ...ConsumerOption) ConsumerOptions { return ConsumerOptions{opts: opts} }
func WithConsumerName(name string) ConsumerNameOption         { return ConsumerNameOption{name} }

// WithShutdownTimeout sets the timeout for handler shutdown when the consumer stops.
// Default is 5 seconds.
func WithShutdownTimeout(d time.Duration) ConsumerShutdownTimeoutOption {
	return ConsumerShutdownTimeoutOption{v: d}
}

// ReconnectBackoffOption controls the exponential backoff used between
// subscription retry attempts.
type ReconnectBackoffOption struct{ initial, max time.Duration }

func (o ReconnectBackoffOption) applyToConsumerOpts(opts *consumerOpts) {
	opts.reconnectBackoffInitial = o.initial
	opts.reconnectBackoffMax = o.max
}

// WithReconnectBackoff sets the exponential backoff used when a subscription
// fails and is retried. initial is clamped to a minimum of 100ms; max is
// clamped to at least initial.
func WithReconnectBackoff(initial, max time.Duration) ReconnectBackoffOption {
	if initial < 100*time.Millisecond {
		initial = 100 * time.Millisecond
	}
	if max < initial {
		max = initial
	}
	return ReconnectBackoffOption{initial: initial, max: max}
}

func newConsumerOpts(opts ...ConsumerOption) consumerOpts {
	options := consumerOpts{
		log:                     slog.Default(),
		startSeq:                1,
		name:                    fmt.Sprintf("consumer-%s", gonanoid.Must(6)),
		shutdownTimeout:         5 * time.Second,
		reconnectBackoffInitial: 1 * time.Second,
		reconnectBackoffMax:     30 * time.Second,
	}
	for _, opt := range opts {
		opt.applyToConsumerOpts(&options)
	}
	return options
}
