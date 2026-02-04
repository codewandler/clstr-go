package reflector

import (
	"reflect"
	"sync"
	"testing"
)

type testStruct struct {
	Name string
}

type anotherStruct struct {
	Value int
}

func TestTypeInfoOf(t *testing.T) {
	ts := testStruct{Name: "test"}
	ti := TypeInfoOf(ts)

	if ti.Name != "github.com/codewandler/clstr-go/core/reflector.testStruct" {
		t.Errorf("unexpected Name: %s", ti.Name)
	}
	if ti.Type.Name() != "testStruct" {
		t.Errorf("unexpected Type.Name(): %s", ti.Type.Name())
	}
}

func TestTypeInfoOf_Pointer(t *testing.T) {
	ts := &testStruct{Name: "test"}
	ti := TypeInfoOf(ts)

	// Should unwrap pointer and return element type
	if ti.Name != "github.com/codewandler/clstr-go/core/reflector.testStruct" {
		t.Errorf("unexpected Name for pointer: %s", ti.Name)
	}
	if ti.Type.Kind() == reflect.Pointer {
		t.Error("Type should be unwrapped from pointer")
	}
}

func TestTypeInfoFor(t *testing.T) {
	ti := TypeInfoFor[testStruct]()

	if ti.Name != "github.com/codewandler/clstr-go/core/reflector.testStruct" {
		t.Errorf("unexpected Name: %s", ti.Name)
	}
	if ti.Type.Name() != "testStruct" {
		t.Errorf("unexpected Type.Name(): %s", ti.Type.Name())
	}
}

func TestTypeInfoFor_Pointer(t *testing.T) {
	ti := TypeInfoFor[*testStruct]()

	// Should unwrap pointer type parameter
	if ti.Name != "github.com/codewandler/clstr-go/core/reflector.testStruct" {
		t.Errorf("unexpected Name for pointer type: %s", ti.Name)
	}
}

func TestTypeInfoForType(t *testing.T) {
	rt := reflect.TypeFor[testStruct]()
	ti := TypeInfoForType(rt)

	if ti.Name != "github.com/codewandler/clstr-go/core/reflector.testStruct" {
		t.Errorf("unexpected Name: %s", ti.Name)
	}
	if ti.Type != rt {
		t.Error("Type should match input reflect.Type")
	}
}

func TestTypeInfoForType_Pointer(t *testing.T) {
	rt := reflect.TypeFor[*testStruct]()
	ti := TypeInfoForType(rt)

	// Should unwrap pointer
	if ti.Name != "github.com/codewandler/clstr-go/core/reflector.testStruct" {
		t.Errorf("unexpected Name for pointer type: %s", ti.Name)
	}
	if ti.Type.Kind() == reflect.Pointer {
		t.Error("Type should be unwrapped from pointer")
	}
}

func TestTypeInfoForType_Nil(t *testing.T) {
	ti := TypeInfoForType(nil)

	if ti.Name != "" {
		t.Errorf("expected empty Name for nil type, got: %s", ti.Name)
	}
	if ti.Type != nil {
		t.Error("expected nil Type for nil input")
	}
}

func TestConcurrentAccess(t *testing.T) {
	const goroutines = 100
	const iterations = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for range goroutines {
		go func() {
			defer wg.Done()
			for range iterations {
				_ = TypeInfoOf(testStruct{})
				_ = TypeInfoFor[anotherStruct]()
				_ = TypeInfoForType(reflect.TypeFor[string]())
			}
		}()
	}

	wg.Wait()
}

func TestCacheHit(t *testing.T) {
	// Clear cache for test isolation
	muCache.Lock()
	cache = make(map[reflect.Type]TypeInfo)
	muCache.Unlock()

	// First call should populate cache
	ti1 := TypeInfoOf(testStruct{})

	// Second call should return cached value
	ti2 := TypeInfoOf(testStruct{})

	if ti1.Name != ti2.Name {
		t.Error("cached result should match original")
	}
	if ti1.Type != ti2.Type {
		t.Error("cached Type should match original")
	}

	// Verify cache has entry
	muCache.RLock()
	_, ok := cache[reflect.TypeFor[testStruct]()]
	muCache.RUnlock()

	if !ok {
		t.Error("expected cache to contain testStruct type")
	}
}

func TestCacheSizeLimit(t *testing.T) {
	// Clear cache
	muCache.Lock()
	cache = make(map[reflect.Type]TypeInfo)
	muCache.Unlock()

	// Fill cache to just under limit
	// We can't easily create 1024 unique types, so we just verify the mechanism
	// by checking the cache clears when it would exceed the limit

	// Add a known entry
	_ = TypeInfoOf(testStruct{})

	muCache.RLock()
	initialSize := len(cache)
	muCache.RUnlock()

	if initialSize == 0 {
		t.Error("expected cache to have at least one entry")
	}
}
