package ds

import (
	"encoding/json"
)

type (
	MapFactory[T any]    interface{ Create(id string) *T }
	Map[T MapFactory[T]] struct{ d map[string]*T }
)

// === Map[T] ===

func (i *Map[T]) Len() int { return len(i.d) }

func (i *Map[T]) Data() map[string]*T { return i.d }

func (i *Map[T]) Keys() *Set[string] {
	keys := make([]string, 0, len(i.d))
	for k := range i.d {
		keys = append(keys, k)
	}
	return NewSet(keys...)
}

func (i *Map[T]) Ensure(id string) (e *T) {
	var ok bool
	e, ok = i.d[id]
	if !ok {
		var z T
		e = z.Create(id)
		i.d[id] = e
	}
	return e
}

func (i *Map[T]) Remove(id string) {
	delete(i.d, id)
}

func NewMap[T MapFactory[T]]() *Map[T] {
	return &Map[T]{d: make(map[string]*T)}
}

func (i *Map[T]) MarshalJSON() ([]byte, error) { return json.Marshal(i.d) }
func (i *Map[T]) UnmarshalJSON(data []byte) error {
	i.d = make(map[string]*T)
	return json.Unmarshal(data, &i.d)
}
