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
	name        string
	mu          sync.RWMutex
	log         *slog.Logger
	state       T
	snapshotter Snapshotter
	version     Version
}

func (i *InMemoryProjection[T]) Name() string     { return i.name }
func (i *InMemoryProjection[T]) Version() Version { return i.version }
func (i *InMemoryProjection[T]) State() T {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.state
}

func (i *InMemoryProjection[T]) Handle(ctx context.Context, env Envelope, event any) error {
	i.log.Debug("projection event", slog.Any("event", event))

	i.mu.Lock()
	defer i.mu.Unlock()

	updated, err := i.state.Apply(ctx, env, event)
	if err != nil {

		return err
	}

	if updated && i.snapshotter != nil {
		var data []byte
		data, err = i.state.Snapshot()
		if err != nil {
			return err
		}
		nextVersion := i.version + 1
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
		i.log.Debug("saved snapshot", nextVersion.SlogAttr())
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
			p.version = s.ObjVersion
			log.Debug("restored projection from snapshot", p.version.SlogAttr())
		}
	}

	return p, nil
}
