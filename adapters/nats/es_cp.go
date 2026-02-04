package nats

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/codewandler/clstr-go/core/es"
	"github.com/codewandler/clstr-go/ports/kv"
)

const defaultCpTimeout = 10 * time.Second

type AggCpStoreConfig struct {
	Connect   Connector
	Bucket    string
	KeyPrefix string
	Timeout   time.Duration // Timeout for checkpoint operations (default: 10s)
}

type AggCpStore struct {
	kv      *KvStore
	timeout time.Duration
}

func NewAggCpStore(cfg AggCpStoreConfig) (*AggCpStore, error) {
	kvStore, err := NewKvStore(KvConfig{
		Bucket:    cfg.Bucket,
		Connect:   cfg.Connect,
		KeyPrefix: cfg.KeyPrefix,
	})
	if err != nil {
		return nil, err
	}

	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = defaultCpTimeout
	}

	return &AggCpStore{kv: kvStore, timeout: timeout}, nil
}

func (c *AggCpStore) getKey(projectionName, aggKey string) string {
	return fmt.Sprintf("proj-%s-%s", projectionName, aggKey)
}

func (c *AggCpStore) Get(ctx context.Context, projectionName, aggKey string) (lastVersion es.Version, err error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	v, err := kv.Get[es.Version](ctx, c.kv, c.getKey(projectionName, aggKey))
	if err != nil {
		if errors.Is(err, kv.ErrNotFound) {
			return 0, nil
		}
		return 0, err
	}
	return v, nil
}

func (c *AggCpStore) Set(ctx context.Context, projectionName, aggKey string, lastVersion es.Version) error {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return kv.Put[es.Version](ctx, c.kv, c.getKey(projectionName, aggKey), lastVersion, kv.PutOptions{})
}

var _ es.AggCpStore = (*AggCpStore)(nil)

//

type CpStoreConfig struct {
	Connect Connector
	Bucket  string
	Key     string
	Timeout time.Duration // Timeout for checkpoint operations (default: 10s)
}

type CpStore struct {
	kv      *KvStore
	key     string
	timeout time.Duration
}

func NewCpStore(cfg CpStoreConfig) (*CpStore, error) {
	if cfg.Bucket == "" {
		return nil, errors.New("bucket is required")
	}
	if cfg.Key == "" {
		return nil, errors.New("key is required")
	}
	kvStore, err := NewKvStore(KvConfig{
		Bucket:  cfg.Bucket,
		Connect: cfg.Connect,
	})
	if err != nil {
		return nil, err
	}

	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = defaultCpTimeout
	}

	return &CpStore{kv: kvStore, key: cfg.Key, timeout: timeout}, nil
}

func (s *CpStore) Get(ctx context.Context) (lastSeq uint64, err error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	lastSeq, err = kv.Get[uint64](ctx, s.kv, s.key)
	if err != nil {
		if errors.Is(err, kv.ErrNotFound) {
			return 0, nil
		}
		return 0, fmt.Errorf("failed to get last seq: %w", err)
	}
	return lastSeq, nil
}

func (s *CpStore) Set(ctx context.Context, lastSeq uint64) error {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	return kv.Put[uint64](ctx, s.kv, s.key, lastSeq, kv.PutOptions{})
}

var _ es.CpStore = (*CpStore)(nil)
