package cache

import "time"

type PutOptions struct {
	TTL time.Duration
}

type PutOption func(*PutOptions)

func WithTTL(ttl time.Duration) PutOption {
	return func(o *PutOptions) {
		o.TTL = ttl
	}
}

type Cache interface {
	Get(key string) (any, bool)
	Put(key string, val any, opts ...PutOption)
	Delete(key string)
}

type TypedCache[T any] interface {
	Put(key string, val T, opts ...PutOption)
	Get(key string) (T, bool)
	Delete(key string)
}

type typedCache[T any] struct {
	c Cache
}

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
