// Package cache provides a simple key-value cache interface with LRU eviction
// and TTL support.
//
// The package defines two interfaces:
//
//   - [Cache]: Untyped cache storing values as any
//   - [TypedCache]: Generic type-safe wrapper via [NewTyped]
//
// # Implementations
//
// [LRU] provides an in-memory LRU cache that is safe for concurrent use.
// It runs a background goroutine for cache operations, ensuring thread safety
// without external locking.
//
//	cache := cache.NewLRU(cache.LRUOpts{Size: 1000})
//	defer cache.Close()
//
//	cache.Put("key", value, cache.WithTTL(5*time.Minute))
//	if val, ok := cache.Get("key"); ok {
//	    // Use val
//	}
//
// # Type-Safe Usage
//
// Use [NewTyped] for compile-time type safety:
//
//	userCache := cache.NewTyped[*User](lruCache)
//	userCache.Put("user:123", user)
//	if user, ok := userCache.Get("user:123"); ok {
//	    // user is *User, no type assertion needed
//	}
//
// # TTL Support
//
// Use [WithTTL] to set per-entry expiration:
//
//	cache.Put("session", data, cache.WithTTL(30*time.Minute))
//
// Expired entries are lazily evicted on access.
package cache
