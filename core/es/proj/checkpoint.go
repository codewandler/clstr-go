package proj

import "sync"

type (
	SubCpStore interface {
		Get() (lastSeq uint64, err error)
		Set(lastSeq uint64) error
	}

	CpStore interface {
		Get(projectionName, aggKey string) (lastVersion int, ok bool)
		Set(projectionName, aggKey string, lastVersion int)
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

type InMemoryCpStore struct {
	mu sync.RWMutex
	m  map[string]map[string]int // proj -> aggID -> version
}

func NewInMemoryCpStore() *InMemoryCpStore {
	return &InMemoryCpStore{m: map[string]map[string]int{}}
}

func (s *InMemoryCpStore) Get(proj, aggID string) (int, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	pm, ok := s.m[proj]
	if !ok {
		return 0, false
	}
	v, ok := pm[aggID]
	return v, ok
}

func (s *InMemoryCpStore) Set(proj, aggID string, v int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pm, ok := s.m[proj]
	if !ok {
		pm = map[string]int{}
		s.m[proj] = pm
	}
	pm[aggID] = v
}
