package es

import (
	"context"
	"errors"
	"sync"
)

var (
	ErrCheckpointNotFound = errors.New("checkpoint not found")
)

type (
	CpStore interface {
		Get(ctx context.Context) (lastSeq uint64, err error)
		Set(ctx context.Context, lastSeq uint64) error
	}

	AggCpStore interface {
		Get(ctx context.Context, projectionName, aggKey string) (lastVersion Version, err error)
		Set(ctx context.Context, projectionName, aggKey string, lastVersion Version) error
	}
)

type (
	AggCpStoreOption valueOption[AggCpStore]
	CpStoreOption    valueOption[CpStore]
)

type InMemCpStore struct {
	mu sync.RWMutex
	v  uint64
}

func NewInMemCpStore() *InMemCpStore {
	return &InMemCpStore{}
}

func (s *InMemCpStore) Get(_ context.Context) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.v, nil
}

func (s *InMemCpStore) Set(_ context.Context, v uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.v = v
	return nil
}

var _ CpStore = (*InMemCpStore)(nil)

//

type InMemAggCpStore struct {
	mu sync.RWMutex
	m  map[string]map[string]Version // proj -> aggID -> version
}

func NewInMemAggCpStore() *InMemAggCpStore {
	return &InMemAggCpStore{m: map[string]map[string]Version{}}
}

func (s *InMemAggCpStore) Get(_ context.Context, proj, aggID string) (Version, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	pm, ok := s.m[proj]
	if !ok {
		return 0, ErrCheckpointNotFound
	}
	v, ok := pm[aggID]
	if !ok {
		return 0, ErrCheckpointNotFound
	}
	return v, nil
}

func (s *InMemAggCpStore) Set(_ context.Context, proj, aggID string, v Version) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	pm, ok := s.m[proj]
	if !ok {
		pm = map[string]Version{}
		s.m[proj] = pm
	}
	pm[aggID] = v
	return nil
}

var _ AggCpStore = (*InMemAggCpStore)(nil)

// === Noop: for memory projections ===

type NoopAggCpStore struct{}

func (NoopAggCpStore) Get(_ context.Context, _, _ string) (Version, error) {
	return 0, ErrCheckpointNotFound
}
func (NoopAggCpStore) Set(_ context.Context, _, _ string, _ Version) error { return nil }

func NewNoopCpStore() AggCpStore { return NoopAggCpStore{} }
