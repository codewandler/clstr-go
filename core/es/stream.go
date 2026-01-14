package es

import (
	"context"

	"github.com/codewandler/clstr-go/core/es/envelope"
)

type DeliverPolicy string

const (
	DeliverAllPolicy DeliverPolicy = "all"
	DeliverNewPolicy DeliverPolicy = "new"
)

type SubscribeFilter struct {
	AggregateType string
	AggregateID   string
}

type SubscribeOpts struct {
	deliverPolicy DeliverPolicy
	filters       []SubscribeFilter
	startSequence uint64
	startVersion  int
}

func (s *SubscribeOpts) DeliverPolicy() DeliverPolicy { return s.deliverPolicy }
func (s *SubscribeOpts) Filters() []SubscribeFilter   { return s.filters }

type SubscribeOption func(opts *SubscribeOpts)

func NewSubscribeOpts(opts ...SubscribeOption) SubscribeOpts {
	options := SubscribeOpts{
		deliverPolicy: DeliverNewPolicy,
	}

	for _, opt := range opts {
		opt(&options)
	}
	return options
}

func WithDeliverPolicy(policy DeliverPolicy) SubscribeOption {
	return func(opts *SubscribeOpts) {
		opts.deliverPolicy = policy
	}
}

func WithFilters(filters ...SubscribeFilter) SubscribeOption {
	return func(opts *SubscribeOpts) {
		opts.filters = filters
	}
}

func WithStartSequence(startSequence uint64) SubscribeOption {
	return func(opts *SubscribeOpts) {
		opts.startSequence = startSequence
	}
}

type Subscription interface {
	Cancel()
	Chan() <-chan envelope.Envelope
}

type Stream interface {
	Subscribe(ctx context.Context, opts ...SubscribeOption) (Subscription, error)
}

func matchFilters(env envelope.Envelope, filters []SubscribeFilter) bool {
	for _, f := range filters {
		if !matchFilter(env, f) {
			return false
		}
	}
	return true
}

func matchFilter(env envelope.Envelope, filter SubscribeFilter) bool {
	if filter.AggregateType != "" {
		if env.Type != filter.AggregateType {
			return false
		}
	}
	if filter.AggregateID != "" {
		if env.AggregateID != filter.AggregateID {
			return false
		}
	}
	return true
}
