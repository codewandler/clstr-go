package proj

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/codewandler/clstr-go/core/es/envelope"
)

type (
	// Projection consumes persisted events to build read models / indexes.
	Projection interface {
		Name() string
		Handle(ctx context.Context, env envelope.Envelope, event any) error
	}

	Projector interface {
		Project(ctx context.Context, p Projection, env envelope.Envelope, event any) error
	}
)

type projector struct {
	log        *slog.Logger
	checkpoint CheckpointStore
}

func NewProjector(log *slog.Logger, cp CheckpointStore) Projector {
	return &projector{log: log, checkpoint: cp}
}

func (dp *projector) Project(ctx context.Context, p Projection, env envelope.Envelope, ev any) error {
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

// ProjectionRunner wires publisher -> registry decode -> projection handlers.
type ProjectionRunner struct {
	mu          sync.RWMutex
	log         *slog.Logger
	decoder     envelope.Decoder
	projections []Projection
	projector   Projector
	subs        map[string][]string
}

func NewProjectionRunner(
	log *slog.Logger,
	decoder envelope.Decoder,
	cp CheckpointStore,
) *ProjectionRunner {
	return &ProjectionRunner{
		log:         log,
		decoder:     decoder,
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

type Handler func(ctx context.Context, envs []envelope.Envelope) error

// Handler returns a bus handler you can subscribe with.
func (r *ProjectionRunner) Handler() Handler {
	return func(ctx context.Context, envelopes []envelope.Envelope) error {
		r.mu.RLock()
		projections := make([]Projection, len(r.projections))
		copy(projections, r.projections)
		r.mu.RUnlock()

		if len(projections) == 0 {
			return nil
		}

		for _, ev := range envelopes {
			evt, err := r.decoder.Decode(ev)
			if err != nil {
				return err
			}

			for _, p := range projections {

				if err := r.projector.Project(ctx, p, ev, evt); err != nil {
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

func (dp *debugProjection) Handle(ctx context.Context, env envelope.Envelope, event any) error {
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
