package es

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
)

type (
	// Projection consumes persisted events to build read models / indexes.
	Projection interface {
		Name() string
		Handle(ctx context.Context, env Envelope, event any) error
	}

	Projector interface {
		Project(ctx context.Context, p Projection, env Envelope, event any) error
	}

	CheckpointStore interface {
		Get(projectionName, aggregateID string) (lastVersion int, ok bool)
		Set(projectionName, aggregateID string, lastVersion int)
	}
)

type InMemoryCheckpointStore struct {
	mu sync.RWMutex
	m  map[string]map[string]int // proj -> aggID -> version
}

func NewInMemoryCheckpointStore() *InMemoryCheckpointStore {
	return &InMemoryCheckpointStore{m: map[string]map[string]int{}}
}

func (s *InMemoryCheckpointStore) Get(proj, aggID string) (int, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	pm, ok := s.m[proj]
	if !ok {
		return 0, false
	}
	v, ok := pm[aggID]
	return v, ok
}

func (s *InMemoryCheckpointStore) Set(proj, aggID string, v int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pm, ok := s.m[proj]
	if !ok {
		pm = map[string]int{}
		s.m[proj] = pm
	}
	pm[aggID] = v
}

type projector struct {
	log        *slog.Logger
	checkpoint CheckpointStore
}

func (dp *projector) Project(ctx context.Context, p Projection, env Envelope, ev any) error {
	name := p.Name()

	log := dp.log.With(
		slog.String("projection", p.Name()),
		slog.Group("event",
			slog.String("aggregate_id", env.AggregateID),
			slog.String("type", env.Type),
			slog.Int("version", env.Version),
		),
		slog.String("go_type", fmt.Sprintf("%T", ev)),
		slog.Any("event", ev),
	)

	// Idempotency check per projection + aggregate stream.
	last, ok := dp.checkpoint.Get(name, env.AggregateID)
	if ok && env.Version <= last {
		log.Warn("skipping projection", slog.Int("last_version", last), slog.Int("current_version", env.Version))
		return nil
	}

	if err := p.Handle(ctx, env, ev); err != nil {
		return fmt.Errorf("projection %s failed to handle envelope=%+v evt=%+v: %w", name, env, ev, err)
	}

	dp.checkpoint.Set(name, env.AggregateID, env.Version)

	return nil
}

func NewProjector(log *slog.Logger, cp CheckpointStore) Projector {
	return &projector{log: log, checkpoint: cp}
}

func NewDefaultProjector() Projector {
	return NewProjector(slog.Default(), NewInMemoryCheckpointStore())
}

// ProjectionRunner wires publisher -> registry decode -> projection handlers.
type ProjectionRunner struct {
	mu          sync.RWMutex
	log         *slog.Logger
	registry    *EventRegistry
	projections []Projection
	projector   Projector
	subs        map[string][]string
}

func NewProjectionRunner(
	log *slog.Logger,
	reg *EventRegistry,
	cp CheckpointStore,
) *ProjectionRunner {
	return &ProjectionRunner{
		log:         log,
		registry:    reg,
		projector:   NewProjector(log, cp),
		projections: make([]Projection, 0),
	}
}

func (r *ProjectionRunner) Register(p Projection) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.projections = append(r.projections, p)
	r.log.Debug("registered projection", slog.String("name", p.Name()))
}

type Handler func(ctx context.Context, envs []Envelope) error

// Handler returns a bus handler you can subscribe with.
func (r *ProjectionRunner) Handler() Handler {
	return func(ctx context.Context, envs []Envelope) error {
		r.mu.RLock()
		projections := make([]Projection, len(r.projections))
		copy(projections, r.projections)
		r.mu.RUnlock()

		if len(projections) == 0 {
			return nil
		}

		for _, env := range envs {
			ev, err := r.registry.Decode(env)
			if err != nil {
				return err
			}

			for _, p := range projections {

				if err := r.projector.Project(ctx, p, env, ev); err != nil {
					return fmt.Errorf("projection handler failed: %w", err)
				}
			}
		}
		return nil
	}
}

type debugProjection struct {
	log *slog.Logger
}

func (dp *debugProjection) Handle(ctx context.Context, env Envelope, event any) error {
	dp.log.Debug(
		"projection event",
		slog.String("kind", fmt.Sprintf("%T", event)),
		slog.Any("aggregate_id", env.AggregateID),
		slog.Any("event", event),
	)
	return nil
}

func (dp *debugProjection) Name() string { return "debug" }

func NewDebugProjection(log *slog.Logger) Projection {
	return &debugProjection{log: log}
}
