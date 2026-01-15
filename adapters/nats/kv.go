package nats

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/codewandler/clstr-go/ports/kv"
)

type KvConfig struct {
	Connect      Connector
	Bucket       string
	MaxBytes     int
	MaxValueSize int
}

type KvStore struct {
	kv jetstream.KeyValue
}

func NewKvStore(cfg KvConfig) (*KvStore, error) {
	if cfg.Bucket == "" {
		return nil, errors.New("bucket is required")
	}

	// set default
	if cfg.MaxValueSize <= 0 {
		cfg.MaxValueSize = -1
	}

	if cfg.MaxBytes <= 0 {
		cfg.MaxBytes = -1
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

	natsKV, err := js.CreateOrUpdateKeyValue(context.Background(), jetstream.KeyValueConfig{
		Bucket:  bucket,
		Storage: jetstream.FileStorage,
		// TODO:
		MaxBytes:       int64(cfg.MaxBytes),
		MaxValueSize:   int32(cfg.MaxValueSize),
		LimitMarkerTTL: 10 * time.Second,
	})

	if err != nil {
		return nil, err
	}

	return &KvStore{kv: natsKV}, nil
}

func (k *KvStore) Put(ctx context.Context, key string, entry kv.Entry, opts kv.PutOptions) (err error) {
	_, err = k.kv.Put(ctx, key, entry.Data)
	if err != nil {
		return err
	}
	if opts.TTL > 0 {
		err = k.kv.Purge(ctx, key, jetstream.PurgeTTL(opts.TTL))
		if err != nil {
			return err
		}
	}
	return nil
}

func (k *KvStore) Get(ctx context.Context, key string) (entry kv.Entry, err error) {
	v, err := k.kv.Get(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return entry, kv.ErrNotFound
		}
		return entry, fmt.Errorf("failed to get checkpoint for %s: %w", key, err)
	}
	entry.Data = v.Value()
	return entry, nil
}

func (k *KvStore) Delete(ctx context.Context, key string) error { return k.kv.Delete(ctx, key) }

var _ kv.Store = (*KvStore)(nil)
