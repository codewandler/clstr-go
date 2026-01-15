package nats

import (
	"github.com/codewandler/clstr-go/core/es"
)

// NewSnapshotter creates a new jetstream key-value-store based snapshotter.
func NewSnapshotter(cfg KvConfig) (*es.KeyValueSnapshotter, error) {
	kv, err := NewKvStore(cfg)
	if err != nil {
		return nil, err
	}
	return es.NewKeyValueSnapshotter(kv), nil
}
