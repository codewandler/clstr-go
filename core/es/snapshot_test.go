package es

import (
	"context"
	"testing"

	"github.com/codewandler/clstr-go/ports/kv"
)

func TestKeyValueSnapshotter_getKey_NoCollision(t *testing.T) {
	s := NewKeyValueSnapshotter(nil) // store not needed for key generation

	// These MUST produce different keys.
	key1 := s.getKey("order", "item-99")
	key2 := s.getKey("order-item", "99")

	if key1 == key2 {
		t.Fatalf("key collision: %q == %q", key1, key2)
	}
}

func TestKeyValueSnapshotter_getKey_Format(t *testing.T) {
	s := NewKeyValueSnapshotter(nil)
	got := s.getKey("user", "abc-123")
	want := "user:abc-123"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestKeyValueSnapshotter_SaveSnapshot_DoesNotOverwriteNewer(t *testing.T) {
	store := kv.NewMemStore()
	snapper := NewKeyValueSnapshotter(store)
	ctx := context.Background()

	// Save a snapshot at StreamSeq=10.
	newerSnap := Snapshot{
		SnapshotID: "snap-newer",
		ObjType:    "user",
		ObjID:      "u1",
		ObjVersion: 10,
		StreamSeq:  10,
		Data:       []byte(`{"name":"newer"}`),
	}
	err := snapper.SaveSnapshot(ctx, newerSnap, SnapshotSaveOpts{})
	if err != nil {
		t.Fatalf("save newer: %v", err)
	}

	// Attempt to save an older snapshot at StreamSeq=5.
	// This simulates a slow goroutine writing a stale snapshot.
	olderSnap := Snapshot{
		SnapshotID: "snap-older",
		ObjType:    "user",
		ObjID:      "u1",
		ObjVersion: 5,
		StreamSeq:  5,
		Data:       []byte(`{"name":"older"}`),
	}
	err = snapper.SaveSnapshot(ctx, olderSnap, SnapshotSaveOpts{})
	if err != nil {
		t.Fatalf("save older: %v", err)
	}

	// Load and verify the newer snapshot was NOT overwritten.
	loaded, err := snapper.LoadSnapshot(ctx, "user", "u1")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if loaded.StreamSeq != 10 {
		t.Fatalf("expected StreamSeq=10 (newer), got %d — older snapshot overwrote newer", loaded.StreamSeq)
	}
	if loaded.SnapshotID != "snap-newer" {
		t.Fatalf("expected snap-newer, got %s", loaded.SnapshotID)
	}
}

func TestKeyValueSnapshotter_SaveSnapshot_OverwritesOlder(t *testing.T) {
	store := kv.NewMemStore()
	snapper := NewKeyValueSnapshotter(store)
	ctx := context.Background()

	// Save a snapshot at StreamSeq=5.
	olderSnap := Snapshot{
		SnapshotID: "snap-old",
		ObjType:    "user",
		ObjID:      "u1",
		ObjVersion: 5,
		StreamSeq:  5,
		Data:       []byte(`{"name":"old"}`),
	}
	err := snapper.SaveSnapshot(ctx, olderSnap, SnapshotSaveOpts{})
	if err != nil {
		t.Fatalf("save older: %v", err)
	}

	// Save a newer snapshot at StreamSeq=10.
	newerSnap := Snapshot{
		SnapshotID: "snap-new",
		ObjType:    "user",
		ObjID:      "u1",
		ObjVersion: 10,
		StreamSeq:  10,
		Data:       []byte(`{"name":"new"}`),
	}
	err = snapper.SaveSnapshot(ctx, newerSnap, SnapshotSaveOpts{})
	if err != nil {
		t.Fatalf("save newer: %v", err)
	}

	// Newer snapshot should be stored.
	loaded, err := snapper.LoadSnapshot(ctx, "user", "u1")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if loaded.StreamSeq != 10 {
		t.Fatalf("expected StreamSeq=10, got %d", loaded.StreamSeq)
	}
	if loaded.SnapshotID != "snap-new" {
		t.Fatalf("expected snap-new, got %s", loaded.SnapshotID)
	}
}

func TestKeyValueSnapshotter_SaveSnapshot_FirstWrite(t *testing.T) {
	store := kv.NewMemStore()
	snapper := NewKeyValueSnapshotter(store)
	ctx := context.Background()

	// First write should always succeed (no existing snapshot).
	snap := Snapshot{
		SnapshotID: "snap-first",
		ObjType:    "order",
		ObjID:      "o1",
		ObjVersion: 1,
		StreamSeq:  1,
		Data:       []byte(`{"total":100}`),
	}
	err := snapper.SaveSnapshot(ctx, snap, SnapshotSaveOpts{})
	if err != nil {
		t.Fatalf("save: %v", err)
	}

	loaded, err := snapper.LoadSnapshot(ctx, "order", "o1")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if loaded.SnapshotID != "snap-first" {
		t.Fatalf("expected snap-first, got %s", loaded.SnapshotID)
	}
}
