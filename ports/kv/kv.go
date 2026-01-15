package kv

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

var (
	ErrNotFound = errors.New("not found")
)

type Entry struct {
	Data []byte
	Meta map[string]any
}

type PutOptions struct {
	TTL time.Duration
}

type Store interface {
	Put(ctx context.Context, key string, entry Entry, opts PutOptions) error
	Get(ctx context.Context, key string) (entry Entry, err error)
	Delete(ctx context.Context, key string) error
}

func Put[T any](ctx context.Context, store Store, key string, v T, opts PutOptions) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return store.Put(ctx, key, Entry{Data: data}, opts)
}

func Get[T any](ctx context.Context, store Store, key string) (out T, err error) {
	entry, err := store.Get(ctx, key)
	if err != nil {
		return
	}
	err = json.Unmarshal(entry.Data, &out)
	if err != nil {
		return
	}
	return
}
