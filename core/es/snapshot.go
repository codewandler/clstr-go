package es

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	gonanoid "github.com/matoous/go-nanoid/v2"

	"github.com/codewandler/clstr-go/ports/kv"
)

var (
	ErrSnapshotterUnconfigured = errors.New("no snapshotter configured")
	ErrSnapshotNotFound        = errors.New("snapshot not found")
)

type (
	SnapshotterOption valueOption[Snapshotter]
	SnapshotOption    valueOption[bool]
	SnapshotTTLOption valueOption[time.Duration]
)

func WithSnapshotter(s Snapshotter) SnapshotterOption     { return SnapshotterOption{v: s} }
func WithSnapshot(b bool) SnapshotOption                  { return SnapshotOption{v: b} }
func WithSnapshotTTL(ttl time.Duration) SnapshotTTLOption { return SnapshotTTLOption{v: ttl} }

func (o SnapshotterOption) applyToEnv(e *envOptions) { e.snapshotter = o.v }

type (
	// Snapshot represents a point-in-time capture of an aggregate or projection state.
	// Snapshots optimize loading by allowing the system to restore state directly
	// instead of replaying all events from the beginning.
	Snapshot struct {
		// SnapshotID is the unique identifier of this snapshot.
		SnapshotID string `json:"snapshot_id"`

		// ObjID is the identifier of the snapshotted object (aggregate ID or projection name).
		ObjID string `json:"obj_id"`
		// ObjType is the type of the snapshotted object (aggregate type or "projection").
		ObjType string `json:"obj_type"`
		// ObjVersion is the version of the object at the time of snapshot.
		ObjVersion Version `json:"obj_version"`

		// StreamSeq is the global stream sequence number at the time of snapshot.
		// When restoring, events after this sequence are replayed.
		StreamSeq uint64 `json:"stream_seq"`

		// CreatedAt is when this snapshot was created.
		CreatedAt time.Time `json:"created_at"`
		// SchemaVersion tracks the snapshot format for migrations.
		SchemaVersion int `json:"schema_version"`
		// Encoding indicates how Data is encoded (typically "json").
		Encoding string `json:"encoding"`
		// Data contains the serialized state.
		Data []byte `json:"data"`
	}

	// SnapshotSaveOpts configures snapshot persistence.
	SnapshotSaveOpts struct {
		// TTL sets the time-to-live for the snapshot. Zero means no expiration.
		TTL time.Duration
	}

	// Snapshottable is implemented by types that support custom snapshot serialization.
	// If not implemented, JSON marshaling is used as a fallback.
	Snapshottable interface {
		// Snapshot serializes the current state to bytes.
		Snapshot() (data []byte, err error)
		// RestoreSnapshot restores state from previously serialized bytes.
		RestoreSnapshot(data []byte) error
	}

	// Snapshotter provides storage operations for snapshots.
	Snapshotter interface {
		// SaveSnapshot persists a snapshot with optional TTL.
		SaveSnapshot(ctx context.Context, snapshot Snapshot, opts SnapshotSaveOpts) error
		// LoadSnapshot retrieves the latest snapshot for an object.
		// Returns ErrSnapshotNotFound if no snapshot exists.
		LoadSnapshot(ctx context.Context, objType, objID string) (Snapshot, error)
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
) (ss Snapshot, err error) {
	if snapshotter == nil {
		return ss, ErrSnapshotterUnconfigured
	}
	ss, err = snapshotter.LoadSnapshot(ctx, aggType, aggID)
	return
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

func CreateSnapshot(agg Aggregate) (ss Snapshot, err error) {
	var data []byte
	s, ok := any(agg).(Snapshottable)
	if ok {
		data, err = s.Snapshot()
	} else {
		data, err = json.Marshal(agg)
	}
	if err != nil {
		return ss, fmt.Errorf("failed to snapshot: %w", err)
	}
	ss = Snapshot{
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

func NewInMemorySnapshotter() *KeyValueSnapshotter {
	return NewKeyValueSnapshotter(kv.NewMemStore())
}

// === KV Snapshotter ===

type KeyValueSnapshotter struct {
	store kv.Store
}

func NewKeyValueSnapshotter(store kv.Store) *KeyValueSnapshotter {
	return &KeyValueSnapshotter{store: store}
}

func (k *KeyValueSnapshotter) getKey(objType, objID string) string {
	return fmt.Sprintf("%s-%s", objType, objID)
}

func (k *KeyValueSnapshotter) SaveSnapshot(ctx context.Context, snapshot Snapshot, opts SnapshotSaveOpts) error {
	return kv.Put(ctx, k.store, k.getKey(snapshot.ObjType, snapshot.ObjID), snapshot, kv.PutOptions{
		TTL: opts.TTL,
	})
}

func (k *KeyValueSnapshotter) LoadSnapshot(ctx context.Context, objType, objID string) (snap Snapshot, err error) {
	snap, err = kv.Get[Snapshot](ctx, k.store, k.getKey(objType, objID))
	if err != nil {
		if errors.Is(err, kv.ErrNotFound) {
			return snap, ErrSnapshotNotFound
		}
		return snap, err
	}
	return snap, nil
}

var _ Snapshotter = (*KeyValueSnapshotter)(nil)
