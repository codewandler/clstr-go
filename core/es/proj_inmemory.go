package es

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	gonanoid "github.com/matoous/go-nanoid/v2"
)

type InMemoryProjectionState interface {
	Snapshot() ([]byte, error)
	Restore(data []byte) error
	Apply(ctx context.Context, env Envelope, event any) (bool, error)
}

type InMemoryProjection[T InMemoryProjectionState] struct {
	name                       string
	mu                         sync.RWMutex
	log                        *slog.Logger
	state                      T
	snapshotter                Snapshotter
	persistedLastSeq           uint64
	persistedProjectionVersion Version
}

func (i *InMemoryProjection[T]) Name() string                { return i.name }
func (i *InMemoryProjection[T]) GetLastSeq() (uint64, error) { return i.persistedLastSeq, nil }
func (i *InMemoryProjection[T]) State() T {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.state
}

func (i *InMemoryProjection[T]) Handle(msgCtx MsgCtx) error {
	ctx, env, event := msgCtx.Context(), msgCtx.Envelope(), msgCtx.Event()
	i.log.Debug("projection event", slog.Uint64("seq", env.Seq), slog.Any("event", event))

	i.mu.Lock()
	defer i.mu.Unlock()

	updated, err := i.state.Apply(ctx, env, event)
	if err != nil {
		return err
	}

	// TODO: do periodic
	if updated && i.snapshotter != nil && env.Seq%10 == 0 {
		println("DO SNAPSHOT")
		var data []byte
		data, err = i.state.Snapshot()
		if err != nil {
			return err
		}
		nextVersion := i.persistedProjectionVersion + 1
		err = i.snapshotter.SaveSnapshot(ctx, Snapshot{
			SnapshotID:    gonanoid.Must(),
			ObjID:         i.name,
			ObjType:       "proj-in-memory",
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
		i.persistedLastSeq = env.Seq
		i.persistedProjectionVersion = nextVersion
		i.log.Debug(
			"saved snapshot",
			i.persistedProjectionVersion.SlogAttr(),
			slog.Uint64("seq", i.persistedLastSeq),
		)
	}

	return nil
}

type InMemoryProjectionOpts struct {
	Name        string
	Snapshotter Snapshotter
	Log         *slog.Logger
}

func NewInMemoryProjection[T InMemoryProjectionState](
	opts InMemoryProjectionOpts,
	state T,
) (*InMemoryProjection[T], error) {
	projName := opts.Name
	if projName == "" {
		return nil, fmt.Errorf("projection name is required")
	}

	log := opts.Log
	if log == nil {
		log = slog.Default()
	}
	log = log.With(slog.String("projection", projName))

	p := &InMemoryProjection[T]{
		name:        projName,
		log:         log,
		snapshotter: opts.Snapshotter,
		state:       state,
	}

	// restore projection data from snapshot
	if opts.Snapshotter != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s, err := opts.Snapshotter.LoadSnapshot(ctx, "proj-in-memory", projName)
		if err != nil && !errors.Is(err, ErrSnapshotNotFound) {
			return nil, err
		}
		if err == nil {
			err = p.state.Restore(s.Data)
			if err != nil {
				return nil, err
			}
			p.persistedProjectionVersion = s.ObjVersion
			p.persistedLastSeq = s.StreamSeq
			log.Debug(
				"restored from snapshot",
				p.persistedProjectionVersion.SlogAttr(),
				slog.Uint64("seq", p.persistedLastSeq),
			)
		}
	}

	return p, nil
}
