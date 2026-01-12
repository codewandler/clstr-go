package cluster

import (
	"context"
	"fmt"
	"sync"
)

type ScopedHandlerOpts struct {
	Extract func(env Envelope) (key string, err error)
	Create  func(key string) (ServerHandlerFunc, error)
}

func NewScopedHandler(opts ScopedHandlerOpts) ServerHandlerFunc {

	var (
		mu       sync.Mutex
		handlers = map[string]ServerHandlerFunc{}
	)

	return func(ctx context.Context, env Envelope) ([]byte, error) {
		// extract key
		k, err := opts.Extract(env)
		if err != nil {
			return nil, err
		}

		if k == "" {
			return nil, fmt.Errorf("key is required")
		}

		// create handler if not exists
		mu.Lock()
		_, ok := handlers[k]
		if !ok {
			if nh, err := opts.Create(k); err != nil {
				mu.Unlock()
				return nil, err
			} else {
				handlers[k] = nh
			}
		}
		mu.Unlock()

		return handlers[k](ctx, env)
	}
}

func NewKeyHandler(createFunc func(key string) (ServerHandlerFunc, error)) ServerHandlerFunc {
	return NewScopedHandler(ScopedHandlerOpts{
		Extract: func(env Envelope) (string, error) {
			key, ok := env.GetHeader(envHeaderKey)
			if !ok {
				return "", fmt.Errorf("missing key")
			}
			return key, nil
		},
		Create: createFunc,
	})
}
