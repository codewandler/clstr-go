package cache

import (
	"testing"
	"time"
)

func TestLRU_Basic(t *testing.T) {
	l := NewLRU(LRUOpts{Size: 2})
	defer l.Close()

	l.Put("a", 1)
	l.Put("b", 2)

	val, ok := l.Get("a")
	if !ok || val != 1 {
		t.Errorf("expected a=1, got %v, %v", val, ok)
	}

	l.Put("c", 3) // should evict "b"

	_, ok = l.Get("b")
	if ok {
		t.Errorf("expected b to be evicted")
	}

	val, ok = l.Get("c")
	if !ok || val != 3 {
		t.Errorf("expected c=3, got %v, %v", val, ok)
	}
}

func TestLRU_Update(t *testing.T) {
	l := NewLRU(LRUOpts{Size: 2})
	defer l.Close()

	l.Put("a", 1)
	l.Put("a", 2)

	val, ok := l.Get("a")
	if !ok || val != 2 {
		t.Errorf("expected a=2, got %v, %v", val, ok)
	}
}

func TestLRU_Promotion(t *testing.T) {
	l := NewLRU(LRUOpts{Size: 2})
	defer l.Close()

	l.Put("a", 1)
	l.Put("b", 2)

	// Promote "a"
	l.Get("a")

	l.Put("c", 3) // should evict "b" because "a" was promoted

	_, ok := l.Get("b")
	if ok {
		t.Errorf("expected b to be evicted")
	}

	_, ok = l.Get("a")
	if !ok {
		t.Errorf("expected a to be present")
	}
}

func TestLRU_Concurrent(t *testing.T) {
	l := NewLRU(LRUOpts{Size: 100})
	defer l.Close()

	const workers = 10
	const ops = 1000

	done := make(chan bool)
	for i := 0; i < workers; i++ {
		go func(workerID int) {
			for j := 0; j < ops; j++ {
				l.Put("key", j)
				l.Get("key")
			}
			done <- true
		}(i)
	}

	for i := 0; i < workers; i++ {
		<-done
	}
}

func TestLRU_Delete(t *testing.T) {
	l := NewLRU(LRUOpts{Size: 2})
	defer l.Close()

	l.Put("a", 1)
	l.Put("b", 2)

	l.Delete("a")

	_, ok := l.Get("a")
	if ok {
		t.Errorf("expected a to be deleted")
	}

	val, ok := l.Get("b")
	if !ok || val != 2 {
		t.Errorf("expected b=2, got %v, %v", val, ok)
	}

	// Delete non-existent key should not panic
	l.Delete("nonexistent")
}

func TestLRU_TTL(t *testing.T) {
	l := NewLRU(LRUOpts{Size: 2})
	defer l.Close()

	l.Put("a", 1, WithTTL(50*time.Millisecond))
	l.Put("b", 2) // no TTL

	val, ok := l.Get("a")
	if !ok || val != 1 {
		t.Errorf("expected a=1 before expiry, got %v, %v", val, ok)
	}

	time.Sleep(60 * time.Millisecond)

	_, ok = l.Get("a")
	if ok {
		t.Errorf("expected a to be expired")
	}

	val, ok = l.Get("b")
	if !ok || val != 2 {
		t.Errorf("expected b=2 (no TTL), got %v, %v", val, ok)
	}
}

func TestLRU_TTL_Update(t *testing.T) {
	l := NewLRU(LRUOpts{Size: 2})
	defer l.Close()

	l.Put("a", 1, WithTTL(50*time.Millisecond))

	time.Sleep(30 * time.Millisecond)

	// Update with new TTL
	l.Put("a", 2, WithTTL(100*time.Millisecond))

	time.Sleep(30 * time.Millisecond)

	// Should still be valid because TTL was refreshed
	val, ok := l.Get("a")
	if !ok || val != 2 {
		t.Errorf("expected a=2 after TTL refresh, got %v, %v", val, ok)
	}
}

func TestLRU_Close(t *testing.T) {
	l := NewLRU(LRUOpts{Size: 2})
	l.Put("a", 1)
	l.Close()

	// Operations after close should not block
	_, ok := l.Get("a")
	if ok {
		t.Errorf("expected Get to return false after Close")
	}

	// Should not panic
	l.Put("b", 2)
	l.Delete("a")
}

func TestLRU_DefaultSize(t *testing.T) {
	l := NewLRU(LRUOpts{}) // Size defaults to 128
	defer l.Close()

	for i := 0; i < 128; i++ {
		l.Put(string(rune('a'+i)), i)
	}

	// First key should still be present
	_, ok := l.Get(string(rune('a')))
	if !ok {
		t.Errorf("expected first key to be present at size 128")
	}

	// Adding one more should evict the oldest
	l.Put("overflow", 999)

	// We can't easily test which was evicted without knowing promotion order,
	// but we can verify the new key is present
	val, ok := l.Get("overflow")
	if !ok || val != 999 {
		t.Errorf("expected overflow=999, got %v, %v", val, ok)
	}
}
