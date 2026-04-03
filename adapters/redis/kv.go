// Package redis provides a Redis-backed implementation of [ports/kv.Store] and
// an ES [es.Snapshotter] built on top of it.
//
// Usage:
//
//	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
//	snapshotter, err := clstrredis.NewSnapshotter(clstrredis.Config{
//	    Client:    client,
//	    KeyPrefix: "myapp:account-123",
//	})
package redis

import (
	"context"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"

	"github.com/codewandler/clstr-go/ports/kv"
)

// Config holds the configuration for a Redis-backed KV store.
type Config struct {
	// Client is the Redis client to use. Required.
	Client *redis.Client

	// KeyPrefix is an optional namespace prefix prepended to every key as
	// "{prefix}:{key}". Use it to isolate stores sharing the same Redis
	// instance, e.g. "acd-snapshots:account-abc123".
	KeyPrefix string
}

// KvStore implements [kv.Store] backed by Redis.
type KvStore struct {
	client    *redis.Client
	keyPrefix string
}

// NewKvStore creates a new Redis-backed KV store from cfg.
// Returns an error if cfg.Client is nil.
func NewKvStore(cfg Config) (*KvStore, error) {
	if cfg.Client == nil {
		return nil, errors.New("redis: Config.Client must not be nil")
	}
	return &KvStore{
		client:    cfg.Client,
		keyPrefix: cfg.KeyPrefix,
	}, nil
}

func (s *KvStore) getKey(key string) string {
	if s.keyPrefix != "" {
		return s.keyPrefix + ":" + key
	}
	return key
}

// Put stores entry under key. When opts.TTL > 0 the key expires after that
// duration; TTL == 0 means the key persists indefinitely.
func (s *KvStore) Put(ctx context.Context, key string, entry kv.Entry, opts kv.PutOptions) error {
	return s.client.Set(ctx, s.getKey(key), entry.Data, opts.TTL).Err()
}

// Get retrieves the entry stored under key.
// Returns [kv.ErrNotFound] when the key does not exist or has expired.
func (s *KvStore) Get(ctx context.Context, key string) (kv.Entry, error) {
	val, err := s.client.Get(ctx, s.getKey(key)).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return kv.Entry{}, kv.ErrNotFound
		}
		return kv.Entry{}, fmt.Errorf("redis: get %q: %w", key, err)
	}
	return kv.Entry{Data: val}, nil
}

// Delete removes the key. It is not an error if the key does not exist.
func (s *KvStore) Delete(ctx context.Context, key string) error {
	return s.client.Del(ctx, s.getKey(key)).Err()
}

var _ kv.Store = (*KvStore)(nil)
