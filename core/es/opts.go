package es

import (
	"context"
	"log/slog"

	"github.com/codewandler/clstr-go/core/reflector"
)

type (
	valueOption[T any] struct{ v T }
	StoreOption        valueOption[EventStore]

	ContextOption       struct{ ctx context.Context }
	MemoryOption        struct{}
	EventRegisterOption struct {
		t    string
		ctor func() any
	}

	LogOption struct {
		l *slog.Logger
	}
	AggregateOption struct {
		aggregates []Aggregate
	}
	MultiOption[T any] struct{ opts []T }
	EnvOpts            MultiOption[EnvOption]
)

func WithInMemory() MemoryOption         { return MemoryOption{} }
func WithStore(s EventStore) StoreOption { return StoreOption{v: s} }

func WithAggregateCheckpointStore(cps AggCpStore) AggCpStoreOption {
	return AggCpStoreOption{v: cps}
}

func WithCheckpointStore(cps CpStore) CpStoreOption { return CpStoreOption{v: cps} }
func WithEvent[T any]() EventRegisterOption {
	t := reflector.TypeInfoFor[T]().Name
	return EventRegisterOption{t: t, ctor: func() any { return any(new(T)) }}
}
func WithCtx(ctx context.Context) ContextOption     { return ContextOption{ctx: ctx} }
func WithLog(l *slog.Logger) LogOption              { return LogOption{l: l} }
func WithAggregates(a ...Aggregate) AggregateOption { return AggregateOption{aggregates: a} }
func WithEnvOpts(opts ...EnvOption) EnvOpts         { return EnvOpts{opts: opts} }

//func WithOpts[T any](opts ...T) MultiOption[T]           { return MultiOption[T]{opts: opts} }

func (o StoreOption) applyToEnv(e *envOptions) { e.store = o.v }
func (o MemoryOption) applyToEnv(e *envOptions) {
	e.store = NewInMemoryStore()
	e.snapshotter = NewInMemorySnapshotter()
}
func (o EventRegisterOption) applyToEnv(e *envOptions) {
	e.events = append(e.events, o)
}
func (o ContextOption) applyToEnv(e *envOptions) {
	e.ctx = o.ctx
}
func (o LogOption) applyToEnv(e *envOptions) {
	e.log = o.l
}
func (o AggregateOption) applyToEnv(e *envOptions) {
	e.aggregates = append(e.aggregates, o.aggregates...)
}
func (o EnvOpts) applyToEnv(e *envOptions) {
	for _, opt := range o.opts {
		opt.applyToEnv(e)
	}
}
