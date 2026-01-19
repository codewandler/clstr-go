package es

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	gonanoid "github.com/matoous/go-nanoid/v2"
)

type (

	// Projection consumes persisted events to build read models / indexes.
	Projection interface {
		Name() string
		Handler
	}
)

// TODO: make this only wrap a general projection instead

type SnapshottableProjection interface {
	Projection
	Snapshottable
}

type SnapshotProjection[T SnapshottableProjection] struct {
	log                        *slog.Logger
	inner                      T
	snapshotter                Snapshotter
	persistedLastSeq           uint64
	persistedProjectionVersion Version
}

func (p *SnapshotProjection[T]) Projection() T               { return p.inner }
func (p *SnapshotProjection[T]) Name() string                { return p.inner.Name() }
func (p *SnapshotProjection[T]) GetLastSeq() (uint64, error) { return p.persistedLastSeq, nil }

func (p *SnapshotProjection[T]) Handle(msgCtx MsgCtx) error {
	seq, env, event := msgCtx.Seq(), msgCtx.Envelope(), msgCtx.Event()

	msgCtx.Log().Debug("projection event", slog.Uint64("seq", env.Seq), slog.Any("event", event))

	err := p.inner.Handle(msgCtx)
	if err != nil {
		return err
	}

	if msgCtx.Live() && seq%10 == 0 {
		err = p.snapshot(msgCtx)
		if err != nil {
			return err
		}
	}

	return nil
}

func (p *SnapshotProjection[T]) snapshot(msgCtx MsgCtx) (err error) {
	ctx, env := msgCtx.Context(), msgCtx.Envelope()

	var data []byte
	data, err = p.inner.Snapshot()
	if err != nil {
		return err
	}
	nextVersion := p.persistedProjectionVersion + 1
	err = p.snapshotter.SaveSnapshot(ctx, Snapshot{
		SnapshotID:    gonanoid.Must(),
		ObjID:         p.Name(),
		ObjType:       "projection",
		ObjVersion:    nextVersion,
		StreamSeq:     env.Seq,
		CreatedAt:     time.Now(),
		SchemaVersion: 0,
		Encoding:      "json",
		Data:          data,
	}, SnapshotSaveOpts{})
	if err != nil {
		return fmt.Errorf("failed to create snapshot: %w", err)
	}
	p.persistedLastSeq = env.Seq
	p.persistedProjectionVersion = nextVersion
	msgCtx.Log().Debug(
		"snapshot created",
		p.persistedProjectionVersion.SlogAttrWithKey("snapshot_version"),
		slog.Uint64("seq", p.persistedLastSeq),
	)
	return nil
}

func (p *SnapshotProjection[T]) restore() error {
	p.log.Debug("restoring projection state")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s, err := p.snapshotter.LoadSnapshot(ctx, "projection", p.Name())
	if err != nil {
		if errors.Is(err, ErrSnapshotNotFound) {
			return nil
		}
		return fmt.Errorf("failed to load snapshot: %w", err)
	}

	p.log.Debug("restoring snapshot", s.ObjVersion.SlogAttrWithKey("snapshot_version"))

	err = p.inner.RestoreSnapshot(s.Data)
	if err != nil {
		return fmt.Errorf("failed to restore: %w", err)
	}
	p.persistedProjectionVersion = s.ObjVersion
	p.persistedLastSeq = s.StreamSeq
	p.log.Debug("restored projection state", slog.Uint64("seq", p.persistedLastSeq), s.ObjVersion.SlogAttrWithKey("snapshot_version"))
	return nil
}

func NewSnapshotProjection[T SnapshottableProjection](
	log *slog.Logger,
	innerProjection T,
	snapshotter Snapshotter,
) (*SnapshotProjection[T], error) {
	if any(innerProjection) == nil {
		return nil, fmt.Errorf("inner projection is required")
	}
	if snapshotter == nil {
		return nil, fmt.Errorf("snapshotter is required")
	}

	p := &SnapshotProjection[T]{
		snapshotter: snapshotter,
		inner:       innerProjection,
		log:         log.With(slog.String("projection", innerProjection.Name())),
	}

	if err := p.restore(); err != nil {
		return nil, err
	}

	return p, nil
}
