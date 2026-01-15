package kv

import (
	"context"
	"sync"
)

type MemStore struct {
	mu   sync.RWMutex
	data map[string]Entry
}

func NewMemStore() *MemStore {
	return &MemStore{data: map[string]Entry{}}
}

func (m *MemStore) Put(_ context.Context, key string, entry Entry, _ PutOptions) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = entry
	return nil
}

func (m *MemStore) Get(_ context.Context, key string) (entry Entry, err error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var ok bool
	entry, ok = m.data[key]
	if !ok {
		return entry, ErrNotFound
	}

	return entry, nil
}

func (m *MemStore) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
	return nil
}

var _ Store = (*MemStore)(nil)
