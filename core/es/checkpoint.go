package es

import (
	"errors"
	"sync"
)

var (
	ErrCheckpointNotFound = errors.New("checkpoint not found")
)

type (
	CpStore interface {
		Get() (lastSeq uint64, err error)
		Set(lastSeq uint64) error
	}

	AggCpStore interface {
		Get(projectionName, aggKey string) (lastVersion Version, err error)
		Set(projectionName, aggKey string, lastVersion Version) error
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

func (s *InMemCpStore) Get() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.v, nil
}

func (s *InMemCpStore) Set(v uint64) error {
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

func (s *InMemAggCpStore) Get(proj, aggID string) (Version, error) {
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

func (s *InMemAggCpStore) Set(proj, aggID string, v Version) error {
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

func (NoopAggCpStore) Get(proj, aggID string) (Version, error) { return 0, ErrCheckpointNotFound }
func (NoopAggCpStore) Set(proj, aggID string, v Version) error { return nil }

func NewNoopCpStore() AggCpStore { return NoopAggCpStore{} }
