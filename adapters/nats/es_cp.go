package nats

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/codewandler/clstr-go/core/es"
)

type CpStoreConfig struct {
	Connect Connector
	Bucket  string
}

type CpStore struct {
	kv *KvStore[es.Version]
}

func NewCpStore(cfg CpStoreConfig) (*CpStore, error) {
	kv, err := NewKvStore[es.Version](KvConfig{
		Bucket:  cfg.Bucket,
		Connect: cfg.Connect,
	})
	if err != nil {
		return nil, err
	}

	return &CpStore{kv: kv}, nil
}

func (c *CpStore) getKey(projectionName, aggKey string) string {
	return strings.Replace("proj-"+projectionName+"-"+aggKey, ":", "-", -1)
}

func (c *CpStore) Get(projectionName, aggKey string) (lastVersion es.Version, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	v, err := c.kv.Get(ctx, c.getKey(projectionName, aggKey))
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			return 0, nil
		}
		return 0, err
	}
	return v, nil
}

func (c *CpStore) Set(projectionName, aggKey string, lastVersion es.Version) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	return c.kv.Set(ctx, c.getKey(projectionName, aggKey), lastVersion)
}

var _ es.CpStore = (*CpStore)(nil)

//

type SubCpStoreConfig struct {
	Connect Connector
	Bucket  string
	Key     string
}

type SubCpStore struct {
	kv  *KvStore[uint64]
	key string
}

func NewSubCpStore(cfg SubCpStoreConfig) (*SubCpStore, error) {
	if cfg.Bucket == "" {
		return nil, errors.New("bucket is required")
	}
	if cfg.Key == "" {
		return nil, errors.New("key is required")
	}
	kv, err := NewKvStore[uint64](KvConfig{
		Bucket:  cfg.Bucket,
		Connect: cfg.Connect,
	})
	if err != nil {
		return nil, err
	}
	return &SubCpStore{kv: kv, key: cfg.Key}, nil
}

func (s *SubCpStore) Get() (lastSeq uint64, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	lastSeq, err = s.kv.Get(ctx, s.key)
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			return 0, nil
		}
		return 0, fmt.Errorf("failed to get last seq: %w", err)
	}
	return lastSeq, nil
}

func (s *SubCpStore) Set(lastSeq uint64) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return s.kv.Set(ctx, s.key, lastSeq)
}

var _ es.SubCpStore = (*SubCpStore)(nil)
