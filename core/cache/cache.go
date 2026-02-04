package cache

import "time"

// PutOptions configures cache entry storage behavior.
type PutOptions struct {
	// TTL specifies how long the entry should live before expiring.
	// Zero means no expiration.
	TTL time.Duration
}

// PutOption is a functional option for configuring cache Put operations.
type PutOption func(*PutOptions)

// WithTTL sets the time-to-live for a cache entry.
// After the TTL expires, the entry will not be returned by Get.
func WithTTL(ttl time.Duration) PutOption {
	return func(o *PutOptions) {
		o.TTL = ttl
	}
}

// Cache is the interface for a key-value cache with string keys and any values.
type Cache interface {
	// Get retrieves a value by key. Returns false if not found or expired.
	Get(key string) (any, bool)
	// Put stores a value with optional TTL.
	Put(key string, val any, opts ...PutOption)
	// Delete removes a value by key.
	Delete(key string)
}

// TypedCache is a type-safe cache interface parameterized by value type T.
type TypedCache[T any] interface {
	// Put stores a value with optional TTL.
	Put(key string, val T, opts ...PutOption)
	// Get retrieves a value by key. Returns false if not found, expired, or wrong type.
	Get(key string) (T, bool)
	// Delete removes a value by key.
	Delete(key string)
}

type typedCache[T any] struct {
	c Cache
}

// NewTyped wraps a Cache with type-safe operations for type T.
// Values stored via the returned TypedCache can only be retrieved as type T.
func NewTyped[T any](c Cache) TypedCache[T] { return &typedCache[T]{c: c} }

func (t *typedCache[T]) Get(key string) (out T, ok bool) {
	var v any
	v, ok = t.c.Get(key)
	if !ok {
		return out, false
	}

	if out, ok = v.(T); !ok {
		return out, false
	}
	return
}

func (t *typedCache[T]) Put(key string, val T, opts ...PutOption) {
	t.c.Put(key, val, opts...)
}

func (t *typedCache[T]) Delete(key string) {
	t.c.Delete(key)
}

var _ TypedCache[any] = (*typedCache[any])(nil)
