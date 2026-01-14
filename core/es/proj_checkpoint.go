package es

import (
	"errors"
	"sync"
)

var (
	ErrCheckpointNotFound = errors.New("checkpoint not found")
)

type (
	SubCpStore interface {
		Get() (lastSeq uint64, err error)
		Set(lastSeq uint64) error
	}

	CpStore interface {
		Get(projectionName, aggKey string) (lastVersion Version, err error)
		Set(projectionName, aggKey string, lastVersion Version) error
	}
)

type InMemorySubCpStore struct {
	mu sync.RWMutex
	v  uint64
}

func NewInMemorySubCpStore() *InMemorySubCpStore {
	return &InMemorySubCpStore{}
}

func (s *InMemorySubCpStore) Get() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.v, nil
}

func (s *InMemorySubCpStore) Set(v uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.v = v
	return nil
}

var _ SubCpStore = (*InMemorySubCpStore)(nil)

//

type InMemoryCpStore struct {
	mu sync.RWMutex
	m  map[string]map[string]Version // proj -> aggID -> version
}

func NewInMemoryCpStore() *InMemoryCpStore {
	return &InMemoryCpStore{m: map[string]map[string]Version{}}
}

func (s *InMemoryCpStore) Get(proj, aggID string) (Version, error) {
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

func (s *InMemoryCpStore) Set(proj, aggID string, v Version) error {
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

var _ CpStore = (*InMemoryCpStore)(nil)
