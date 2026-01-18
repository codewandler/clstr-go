package es

import (
	"fmt"
	"log/slog"

	gonanoid "github.com/matoous/go-nanoid/v2"
)

type (
	consumerOpts struct {
		startSeq uint64
		mws      []HandlerMiddleware
		log      *slog.Logger
		name     string
	}

	ConsumerOption interface {
		applyToConsumerOpts(*consumerOpts)
	}

	ConsumerNameOption valueOption[string]
	MiddlewareOption   valueOption[[]HandlerMiddleware]
	ConsumerOptions    MultiOption[ConsumerOption]
)

func (o ConsumerNameOption) applyToConsumerOpts(opts *consumerOpts) { opts.name = o.v }
func (o StartSeqOption) applyToConsumerOpts(opts *consumerOpts)     { opts.startSeq = o.v }
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

func newConsumerOpts(opts ...ConsumerOption) consumerOpts {
	options := consumerOpts{
		log:      slog.Default(),
		startSeq: 1,
		name:     fmt.Sprintf("consumer-%s", gonanoid.Must(6)),
	}
	for _, opt := range opts {
		opt.applyToConsumerOpts(&options)
	}
	return options
}
