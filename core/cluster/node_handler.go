package cluster

import (
	"context"
	"sync"

	"github.com/codewandler/clstr-go/core/cache"
)

const DefaultHandlerCacheSize = 1024

type ScopedHandlerOpts struct {
	Extract func(env Envelope) (key string, err error)
	Create  func(key string) (ServerHandlerFunc, error)
	// CacheSize limits the number of cached handlers.
	// 0 = default (1024), -1 = unlimited (original behavior)
	CacheSize int
}

func NewScopedHandler(opts ScopedHandlerOpts) ServerHandlerFunc {
	cacheSize := opts.CacheSize
	if cacheSize == 0 {
		cacheSize = DefaultHandlerCacheSize
	}
	if cacheSize < 0 {
		return newUnboundedScopedHandler(opts)
	}

	lru := cache.NewLRU(cache.LRUOpts{Size: cacheSize})

	return func(ctx context.Context, env Envelope) ([]byte, error) {
		k, err := opts.Extract(env)
		if err != nil {
			return nil, err
		}

		if k == "" {
			return nil, ErrKeyRequired
		}

		if v, ok := lru.Get(k); ok {
			return v.(ServerHandlerFunc)(ctx, env)
		}

		nh, err := opts.Create(k)
		if err != nil {
			return nil, err
		}
		lru.Put(k, nh)
		return nh(ctx, env)
	}
}

func newUnboundedScopedHandler(opts ScopedHandlerOpts) ServerHandlerFunc {
	var (
		mu       sync.Mutex
		handlers = map[string]ServerHandlerFunc{}
	)

	return func(ctx context.Context, env Envelope) ([]byte, error) {
		k, err := opts.Extract(env)
		if err != nil {
			return nil, err
		}

		if k == "" {
			return nil, ErrKeyRequired
		}

		mu.Lock()
		h, ok := handlers[k]
		if !ok {
			nh, err := opts.Create(k)
			if err != nil {
				mu.Unlock()
				return nil, err
			}
			handlers[k] = nh
			h = nh
		}
		mu.Unlock()

		return h(ctx, env)
	}
}

func NewKeyHandler(createFunc func(key string) (ServerHandlerFunc, error)) ServerHandlerFunc {
	return NewKeyHandlerWithOpts(createFunc, 0)
}

func NewKeyHandlerWithOpts(createFunc func(key string) (ServerHandlerFunc, error), cacheSize int) ServerHandlerFunc {
	return NewScopedHandler(ScopedHandlerOpts{
		Extract: func(env Envelope) (string, error) {
			key, ok := env.GetHeader(envHeaderKey)
			if !ok {
				return "", ErrMissingKeyHeader
			}
			return key, nil
		},
		Create:    createFunc,
		CacheSize: cacheSize,
	})
}
