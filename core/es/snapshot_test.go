package es

import "testing"

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
