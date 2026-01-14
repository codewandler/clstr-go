package nats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

var (
	ErrKeyNotFound = errors.New("key not found")
)

type KvConfig struct {
	Connect Connector
	Bucket  string
}

type KvStore[T any] struct {
	kv jetstream.KeyValue
}

func NewKvStore[T any](cfg KvConfig) (*KvStore[T], error) {
	if cfg.Bucket == "" {
		return nil, errors.New("bucket is required")
	}

	doConnect := cfg.Connect
	if doConnect == nil {
		doConnect = ConnectDefault()
	}

	nc, _, err := doConnect()
	if err != nil {
		return nil, err
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}

	bucket := cfg.Bucket
	if bucket == "" {
		bucket = "es_checkpoint_store"
	}

	kv, err := js.CreateOrUpdateKeyValue(context.Background(), jetstream.KeyValueConfig{
		Bucket:   bucket,
		Storage:  jetstream.FileStorage,
		MaxBytes: 1024 * 1024,
	})

	if err != nil {
		return nil, err
	}

	return &KvStore[T]{kv: kv}, nil
}

func (k *KvStore[T]) Set(key string, v T) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	data, err := json.Marshal(v)
	if err != nil {
		return err
	}

	_, err = k.kv.Put(ctx, key, data)
	if err != nil {
		return err
	}
	return nil
}

func (k *KvStore[T]) Get(key string) (out T, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	v, err := k.kv.Get(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return out, ErrKeyNotFound
		}
		return out, fmt.Errorf("failed to get checkpoint for %s: %w", key, err)
	}
	err = json.Unmarshal(v.Value(), &out)
	if err != nil {
		return out, err
	}
	return out, nil
}
