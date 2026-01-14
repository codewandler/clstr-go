package proj

import (
	"errors"
	"sync"

	"github.com/codewandler/clstr-go/core/es/types"
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
		Get(projectionName, aggKey string) (lastVersion types.Version, err error)
		Set(projectionName, aggKey string, lastVersion types.Version) error
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
	m  map[string]map[string]types.Version // proj -> aggID -> version
}

func NewInMemoryCpStore() *InMemoryCpStore {
	return &InMemoryCpStore{m: map[string]map[string]types.Version{}}
}

func (s *InMemoryCpStore) Get(proj, aggID string) (types.Version, error) {
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

func (s *InMemoryCpStore) Set(proj, aggID string, v types.Version) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	pm, ok := s.m[proj]
	if !ok {
		pm = map[string]types.Version{}
		s.m[proj] = pm
	}
	pm[aggID] = v
	return nil
}

var _ CpStore = (*InMemoryCpStore)(nil)
