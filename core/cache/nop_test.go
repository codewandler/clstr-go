package cache

import "testing"

func TestNop(t *testing.T) {
	n := NewNop()
	n.Put("key", "val")
	val, ok := n.Get("key")
	if ok {
		t.Errorf("expected ok to be false, got true")
	}
	if val != nil {
		t.Errorf("expected val to be nil, got %v", val)
	}
}

func TestNop_Delete(t *testing.T) {
	n := NewNop()
	n.Put("key", "val")
	n.Delete("key") // should not panic
	val, ok := n.Get("key")
	if ok {
		t.Errorf("expected ok to be false, got true")
	}
	if val != nil {
		t.Errorf("expected val to be nil, got %v", val)
	}
}
