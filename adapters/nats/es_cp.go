package nats

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/codewandler/clstr-go/core/es"
	"github.com/codewandler/clstr-go/ports/kv"
)

type AggCpStoreConfig struct {
	Connect   Connector
	Bucket    string
	KeyPrefix string
}

type AggCpStore struct {
	kv *KvStore
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

	return &AggCpStore{kv: kvStore}, nil
}

func (c *AggCpStore) getKey(projectionName, aggKey string) string {
	return fmt.Sprintf("proj-%s-%s", projectionName, aggKey)
}

func (c *AggCpStore) Get(projectionName, aggKey string) (lastVersion es.Version, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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

func (c *AggCpStore) Set(projectionName, aggKey string, lastVersion es.Version) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return kv.Put[es.Version](ctx, c.kv, c.getKey(projectionName, aggKey), lastVersion, kv.PutOptions{})
}

var _ es.AggCpStore = (*AggCpStore)(nil)

//

type CpStoreConfig struct {
	Connect Connector
	Bucket  string
	Key     string
}

type CpStore struct {
	kv  *KvStore
	key string
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
	return &CpStore{kv: kvStore, key: cfg.Key}, nil
}

func (s *CpStore) Get() (lastSeq uint64, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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

func (s *CpStore) Set(lastSeq uint64) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return kv.Put[uint64](ctx, s.kv, s.key, lastSeq, kv.PutOptions{})
}

var _ es.CpStore = (*CpStore)(nil)
