package es

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/codewandler/clstr-go/core/es/types"
	gonanoid "github.com/matoous/go-nanoid/v2"
)

var (
	ErrSnapshotterUnconfigured = errors.New("no snapshotter configured")
	ErrSnapshotNotFound        = errors.New("snapshot not found")
)

type (
	Snapshot struct {
		SnapshotID string `json:"snapshot_id"` // SnapshotID is the unique ID of the snapshot

		ObjID      string        `json:"obj_id"`      // ObjectID is the ID of the object that was snapshotted
		ObjType    string        `json:"obj_type"`    // ObjectType is the type of the object that was snapshotted
		ObjVersion types.Version `json:"obj_version"` // Version is the version of the object at the time of snapshot

		StreamSeq uint64 `json:"stream_seq"` // StreamSeq is the global sequence number from the store

		CreatedAt     time.Time `json:"created_at"`
		SchemaVersion int       `json:"schema_version"`
		Encoding      string    `json:"encoding"`
		Data          []byte    `json:"data"`
	}

	Snapshottable interface {
		Snapshot() (data []byte, err error)
		RestoreSnapshot(data []byte) error
	}

	Snapshotter interface {
		SaveSnapshot(ctx context.Context, snapshot *Snapshot) error
		LoadSnapshot(ctx context.Context, objType, objID string) (*Snapshot, error)
	}
)

func (s *Snapshot) logAttrs() slog.Attr {
	return slog.Group(
		"snapshot",
		slog.String("id", s.SnapshotID),
		slog.String("obj_type", s.ObjType),
		slog.String("obj_id", s.ObjID),
		s.ObjVersion.SlogAttrWithKey("obj_version"),
		slog.Uint64("seq", s.StreamSeq),
		slog.Time("created_at", s.CreatedAt),

		slog.Int("size", len(s.Data)),
	)
}

func LoadSnapshot(
	ctx context.Context,
	snapshotter Snapshotter,
	aggType, aggID string,
) (*Snapshot, error) {
	if snapshotter == nil {
		return nil, ErrSnapshotterUnconfigured
	}
	return snapshotter.LoadSnapshot(ctx, aggType, aggID)
}

func ApplySnapshot(ctx context.Context, snapshotter Snapshotter, agg Aggregate) (err error) {
	snapshot, err := LoadSnapshot(ctx, snapshotter, agg.GetAggType(), agg.GetID())
	if err != nil {
		return err
	}
	if sss, ok := any(agg).(Snapshottable); ok {
		err = sss.RestoreSnapshot(snapshot.Data)
	} else {
		err = json.Unmarshal(snapshot.Data, agg)
	}
	if err != nil {
		return fmt.Errorf("failed to restore snapshot: %w", err)
	}
	agg.setVersion(snapshot.ObjVersion)
	agg.setSeq(snapshot.StreamSeq)
	return nil
}

func CreateSnapshot(agg Aggregate) (ss *Snapshot, err error) {
	var data []byte
	s, ok := any(agg).(Snapshottable)
	if ok {
		data, err = s.Snapshot()
	} else {
		data, err = json.Marshal(agg)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to createAndSafeSnapshotForAgg: %w", err)
	}
	ss = &Snapshot{
		SnapshotID:    gonanoid.Must(),
		StreamSeq:     agg.GetSeq(),
		ObjID:         agg.GetID(),
		ObjType:       agg.GetAggType(),
		ObjVersion:    agg.GetVersion(),
		CreatedAt:     time.Now(),
		Encoding:      "json",
		Data:          data,
		SchemaVersion: 1,
	}
	return
}

// === In-Memory Snapshotter ===

type InMemorySnapshotter struct {
	mu        sync.Mutex
	log       *slog.Logger
	snapshots map[string]*Snapshot
}

func NewInMemorySnapshotter(log *slog.Logger) *InMemorySnapshotter {
	return &InMemorySnapshotter{
		log:       log.With(slog.String("snapshotter", "memory")),
		snapshots: map[string]*Snapshot{},
	}
}

func (i *InMemorySnapshotter) SaveSnapshot(_ context.Context, snapshot *Snapshot) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	sk := fmt.Sprintf("%s-%s", snapshot.ObjType, snapshot.ObjID)
	i.snapshots[sk] = snapshot
	return nil
}

func (i *InMemorySnapshotter) LoadSnapshot(_ context.Context, objType, objID string) (*Snapshot, error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	sk := fmt.Sprintf("%s-%s", objType, objID)
	s, ok := i.snapshots[sk]
	if !ok {
		return nil, ErrSnapshotNotFound
	}
	return s, nil
}

var _ Snapshotter = &InMemorySnapshotter{}
