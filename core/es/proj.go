package es

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
)

type (
	ProjectionEventHandler interface {
		Handle(ctx context.Context, env Envelope, event any) error
	}

	// Projection consumes persisted events to build read models / indexes.
	Projection interface {
		Name() string
		ProjectionEventHandler
	}

	Projector interface {
		Project(ctx context.Context, p Projection, env Envelope, event any) error
	}
)

type projector struct {
	log     *slog.Logger
	cpStore CpStore
}

func NewProjector(log *slog.Logger, cpStore CpStore) Projector {
	return &projector{log: log, cpStore: cpStore}
}

func (dp *projector) Project(ctx context.Context, p Projection, env Envelope, ev any) error {
	name := p.Name()

	/*log := dp.log.With(
		slog.String("projection", p.Name()),
		slog.Group("event",
			slog.String("aggregate_id", env.AggregateID),
			slog.String("type", env.Type),
			slog.Int("version", env.Version),
		),
		slog.String("go_type", fmt.Sprintf("%T", ev)),
		slog.Any("event", ev),
	)*/

	// Idempotency check per projection + aggregate stream.
	aggKey := fmt.Sprintf("%s:%s", env.AggregateType, env.AggregateID)
	last, err := dp.cpStore.Get(name, aggKey)
	if err != nil && !errors.Is(err, ErrCheckpointNotFound) {
		return fmt.Errorf("failed to get checkpoint for projection %s: %w", name, err)
	}

	// skip
	if env.Version <= last {
		return nil
	}

	if err := p.Handle(ctx, env, ev); err != nil {
		return fmt.Errorf("projection %s failed to handle envelope=%+v evt=%+v: %w", name, env, ev, err)
	}

	dp.cpStore.Set(name, aggKey, env.Version)

	return nil
}

// ProjectionRunner wires publisher -> registry decode -> projection handlers.
type ProjectionRunner struct {
	mu          sync.RWMutex
	log         *slog.Logger
	decoder     Decoder
	projections []Projection
	projector   Projector
	scp         SubCpStore
}

func NewProjectionRunner(
	log *slog.Logger,
	decoder Decoder,
	scp SubCpStore,
	cp CpStore,
) *ProjectionRunner {
	return &ProjectionRunner{
		log:         log,
		decoder:     decoder,
		projector:   NewProjector(log, cp),
		projections: make([]Projection, 0),
		scp:         scp,
	}
}

func (r *ProjectionRunner) GetStartSeq() (uint64, error) { return r.scp.Get() }

func (r *ProjectionRunner) Register(p Projection) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.projections = append(r.projections, p)
	r.log.Debug("registered projection", slog.String("name", p.Name()))
}

type Handler func(ctx context.Context, envs []Envelope) error

// Handler returns a bus handler you can subscribe with.
func (r *ProjectionRunner) Handler() Handler {
	return func(ctx context.Context, envelopes []Envelope) error {
		r.mu.RLock()
		projections := make([]Projection, len(r.projections))
		copy(projections, r.projections)
		r.mu.RUnlock()

		if len(projections) == 0 {
			return nil
		}

		var lastSeq uint64

		for _, ev := range envelopes {
			evt, err := r.decoder.Decode(ev)
			if err != nil {
				r.log.Error("failed to decode", slog.Any("error", err))
				continue
			}

			// TODO: concurrency control
			for _, p := range projections {
				if err := r.projector.Project(ctx, p, ev, evt); err != nil {
					r.log.Error("projection failed", slog.String("projection", p.Name()), slog.Any("error", err))
					continue
				}
			}

			lastSeq = ev.Seq
		}

		if err := r.scp.Set(lastSeq); err != nil {
			return fmt.Errorf("failed to set checkpoint: %w", err)
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
