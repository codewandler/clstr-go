package proj

import "sync"

type (
	CheckpointStore interface {
		Get(projectionName, aggregateID string) (lastVersion int, ok bool)
		Set(projectionName, aggregateID string, lastVersion int)
	}
)

type InMemoryCheckpointStore struct {
	mu sync.RWMutex
	m  map[string]map[string]int // proj -> aggID -> version
}

func NewInMemoryCheckpointStore() *InMemoryCheckpointStore {
	return &InMemoryCheckpointStore{m: map[string]map[string]int{}}
}

func (s *InMemoryCheckpointStore) Get(proj, aggID string) (int, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	pm, ok := s.m[proj]
	if !ok {
		return 0, false
	}
	v, ok := pm[aggID]
	return v, ok
}

func (s *InMemoryCheckpointStore) Set(proj, aggID string, v int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pm, ok := s.m[proj]
	if !ok {
		pm = map[string]int{}
		s.m[proj] = pm
	}
	pm[aggID] = v
}
