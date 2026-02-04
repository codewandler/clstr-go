// Package reflector provides type reflection utilities with caching.
// It extracts and caches type metadata for efficient repeated lookups.
package reflector

import (
	"reflect"
	"sync"
)

// maxCacheSize is the maximum number of entries in the type cache.
// Since the number of types in a typical Go program is bounded and small,
// this limit is rarely hit. When exceeded, the cache is cleared.
const maxCacheSize = 1024

var (
	muCache sync.RWMutex
	cache   = make(map[reflect.Type]TypeInfo)
)

// TypeInfo holds metadata about a reflected type.
type TypeInfo struct {
	Name string       // Fully qualified name: "pkg/path.TypeName"
	Type reflect.Type // The underlying reflect.Type
}

// TypeInfoOf returns TypeInfo for the dynamic type of x.
// The result is cached for subsequent lookups.
func TypeInfoOf(x any) TypeInfo {
	return TypeInfoForType(reflect.TypeOf(x))
}

// TypeInfoFor returns TypeInfo for type parameter T.
// The result is cached for subsequent lookups.
func TypeInfoFor[T any]() TypeInfo {
	return TypeInfoForType(reflect.TypeFor[T]())
}

// TypeInfoForType returns TypeInfo for the given reflect.Type.
// For pointer types, returns info about the element type.
// Results are cached; thread-safe for concurrent use.
func TypeInfoForType(t reflect.Type) TypeInfo {
	if t == nil {
		return TypeInfo{}
	}

	// Unwrap pointer before cache lookup to ensure consistent caching
	origType := t
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}

	// Check cache
	muCache.RLock()
	ti, ok := cache[t]
	muCache.RUnlock()
	if ok {
		return ti
	}

	// Build TypeInfo
	ti = TypeInfo{
		Name: t.PkgPath() + "." + t.Name(),
		Type: t,
	}

	// Store in cache with size limit check
	muCache.Lock()
	// Double-check after acquiring write lock
	if existing, ok := cache[origType]; ok {
		muCache.Unlock()
		return existing
	}
	if len(cache) >= maxCacheSize {
		// Clear cache when limit is reached (rare for typical programs)
		cache = make(map[reflect.Type]TypeInfo)
	}
	cache[t] = ti
	muCache.Unlock()

	return ti
}
