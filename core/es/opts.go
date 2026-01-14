package es

import (
	"context"
	"log/slog"

	"github.com/codewandler/clstr-go/core/es/proj"
	"github.com/codewandler/clstr-go/internal/reflector"
)

type (
	valueOption[T any]    struct{ v T }
	StoreOption           valueOption[EventStore]
	CheckpointStoreOption valueOption[proj.CheckpointStore]
	ContextOption         struct{ ctx context.Context }
	MemoryOption          struct{}
	EventRegisterOption   struct {
		t    string
		ctor func() any
	}
	ProjectionsOption struct {
		ps []proj.Projection
	}
	LogOption struct {
		l *slog.Logger
	}
	AggregateOption struct {
		aggregates []Aggregate
	}
	MultiOption[T any] struct{ opts []T }
	EnvOpts            MultiOption[EnvOption]
	SnapshotterOption  valueOption[Snapshotter]
	SnapshotOption     valueOption[bool]
)

func WithInMemory() MemoryOption                      { return MemoryOption{} }
func WithStore(s EventStore) StoreOption              { return StoreOption{v: s} }
func WithSnapshotter(s Snapshotter) SnapshotterOption { return SnapshotterOption{v: s} }
func WithSnapshot(b bool) SnapshotOption              { return SnapshotOption{v: b} }
func WithCheckpointStore(cps proj.CheckpointStore) CheckpointStoreOption {
	return CheckpointStoreOption{v: cps}
}
func WithEvent[T any]() EventRegisterOption {
	t := reflector.TypeInfoFor[T]().Name
	return EventRegisterOption{t: t, ctor: func() any { return any(new(T)) }}
}
func WithProjections(ps ...proj.Projection) ProjectionsOption { return ProjectionsOption{ps: ps} }
func WithCtx(ctx context.Context) ContextOption               { return ContextOption{ctx: ctx} }
func WithLog(l *slog.Logger) LogOption                        { return LogOption{l: l} }
func WithAggregates(a ...Aggregate) AggregateOption           { return AggregateOption{aggregates: a} }
func WithEnvOpts(opts ...EnvOption) EnvOpts                   { return EnvOpts{opts: opts} }

//func WithOpts[T any](opts ...T) MultiOption[T]           { return MultiOption[T]{opts: opts} }

func (o StoreOption) applyToEnv(e *envOptions)           { e.store = o.v }
func (o CheckpointStoreOption) applyToEnv(e *envOptions) { e.checkpointStore = o.v }
func (o MemoryOption) applyToEnv(e *envOptions) {
	e.store = NewInMemoryStore()
	e.checkpointStore = proj.NewInMemoryCheckpointStore()
}
func (o EventRegisterOption) applyToEnv(e *envOptions) {
	e.events = append(e.events, o)
}
func (o ProjectionsOption) applyToEnv(e *envOptions) {
	e.projections = append(e.projections, o.ps...)
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
func (o SnapshotterOption) applyToEnv(e *envOptions) {
	e.snapshotter = o.v
}
