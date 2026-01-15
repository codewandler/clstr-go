package cache

import (
	"testing"
)

func TestLRU_Basic(t *testing.T) {
	l := NewLRU(LRUOpts{Size: 2})

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

	l.Put("a", 1)
	l.Put("a", 2)

	val, ok := l.Get("a")
	if !ok || val != 2 {
		t.Errorf("expected a=2, got %v, %v", val, ok)
	}
}

func TestLRU_Promotion(t *testing.T) {
	l := NewLRU(LRUOpts{Size: 2})

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
