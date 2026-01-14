package nats

import (
	"errors"
	"fmt"

	"github.com/codewandler/clstr-go/core/es/proj"
	"github.com/codewandler/clstr-go/core/es/types"
)

type CpStoreConfig struct {
	Connect Connector
	Bucket  string
}

type CpStore struct {
	kv *KvStore[types.Version]
}

func NewCpStore(cfg CpStoreConfig) (*CpStore, error) {
	kv, err := NewKvStore[types.Version](KvConfig{
		Bucket:  cfg.Bucket,
		Connect: cfg.Connect,
	})
	if err != nil {
		return nil, err
	}

	return &CpStore{kv: kv}, nil
}

func (c *CpStore) getKey(projectionName, aggKey string) string {
	return "proj-" + projectionName + "-" + aggKey
}

func (c *CpStore) Get(projectionName, aggKey string) (lastVersion types.Version, err error) {
	v, err := c.kv.Get(c.getKey(projectionName, aggKey))
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			return 0, nil
		}
		return 0, err
	}
	return v, nil
}

func (c *CpStore) Set(projectionName, aggKey string, lastVersion types.Version) error {
	return c.kv.Set(c.getKey(projectionName, aggKey), lastVersion)
}

var _ proj.CpStore = (*CpStore)(nil)

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
	lastSeq, err = s.kv.Get(s.key)
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			return 0, nil
		}
		return 0, fmt.Errorf("failed to get last seq: %w", err)
	}
	return lastSeq, nil
}

func (s *SubCpStore) Set(lastSeq uint64) error { return s.kv.Set(s.key, lastSeq) }

var _ proj.SubCpStore = (*SubCpStore)(nil)
