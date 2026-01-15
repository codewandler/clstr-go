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
}

type TypedCache[T any] interface {
	Put(key string, val T, opts ...PutOption)
	Get(key string) (T, bool)
}

type typedCache[T any] struct {
	c Cache
}

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

var _ TypedCache[any] = (*typedCache[any])(nil)
