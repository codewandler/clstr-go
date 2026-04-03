package redis

import (
	"github.com/codewandler/clstr-go/core/es"
)

// NewSnapshotter creates an ES snapshotter backed by Redis.
// It wraps a [KvStore] in [es.NewKeyValueSnapshotter], providing the same
// interface as the NATS-backed snapshotter but storing snapshots in Redis
// instead of NATS JetStream KV — reducing JetStream load under high concurrency.
func NewSnapshotter(cfg Config) (*es.KeyValueSnapshotter, error) {
	store, err := NewKvStore(cfg)
	if err != nil {
		return nil, err
	}
	return es.NewKeyValueSnapshotter(store), nil
}
