package ds

import (
	"encoding/json"
	"fmt"
)

type StringSet = Set[string]

type Set[T comparable] struct {
	items map[T]struct{}
	order []T // preserves insertion order
}

func (s *Set[T]) String() string {
	return fmt.Sprintf("%v", s.order)
}

func (s *Set[T]) Add(id T) { // Add adds the given id to the set.
	if s.contains(id) {
		return
	}
	s.items[id] = struct{}{}
	s.order = append(s.order, id)
}
func (s *Set[T]) Extend(ids ...T) *Set[T] {
	additions := s.Additions(NewSet(ids...))
	for _, id := range additions.Values() {
		s.Add(id)
	}
	return additions
}
func (s *Set[T]) Len() int { return len(s.items) }
func (s *Set[T]) Remove(ids ...T) {
	for _, id := range ids {
		if _, ok := s.items[id]; ok {
			delete(s.items, id)
			// remove from order slice while preserving order
			for i, v := range s.order {
				if v == id {
					s.order = append(s.order[:i], s.order[i+1:]...)
					break
				}
			}
		}
	}
} // Remove removes the given ids from the set.

func (s *Set[T]) ContainsValues(ids ...T) bool {
	return s.ContainsAll(NewSet(ids...))
}

func (s *Set[T]) ContainsAny(other *Set[T]) bool {
	for _, id := range other.order {
		if s.contains(id) {
			return true
		}
	}
	return false
}

func (s *Set[T]) ContainsAll(other *Set[T]) bool {
	for id := range other.items {
		if !s.contains(id) {
			return false
		}
	}
	return true
}

func (s *Set[T]) Contains(v T) bool {
	return s.contains(v)
}

func (s *Set[T]) contains(id T) bool {
	_, ok := s.items[id]
	return ok
}

// Additions returns a new set that contains all elements that are present in the
// other set but not in the receiver (s). In other words: the elements you need
// to add to s to obtain other. The order follows the other's insertion order.
func (s *Set[T]) Additions(other *Set[T]) (add *Set[T]) {
	add = NewSet[T]()
	for _, id := range other.order {
		if !s.ContainsValues(id) {
			add.Add(id)
		}
	}
	return
}

func (s *Set[T]) Removals(other *Set[T]) (remove *Set[T]) {
	remove = NewSet[T]()
	for _, id := range s.order {
		if !other.ContainsValues(id) {
			remove.Add(id)
		}
	}
	return
}

// Diff computes the transition needed to go from the current set (s) to the
// target set (other). It returns two ordered sets:
//   - add:    elements to add to s (present in other but not in s), ordered by other's insertion order
//   - remove: elements to remove from s (present in s but not in other), ordered by s's insertion order
func (s *Set[T]) Diff(other *Set[T]) (add *Set[T], remove *Set[T]) {
	return s.Additions(other), s.Removals(other)
}

// Intersect returns a new set that contains all elements that are present in both
// the receiver (s) and the other set. The order of the resulting set preserves
// the insertion order of the receiver (left-hand) set.
func (s *Set[T]) Intersect(other *Set[T]) *Set[T] {
	intersect := NewSet[T]()
	for _, id := range s.order {
		if other.ContainsValues(id) {
			intersect.Add(id)
		}
	}
	return intersect
}
func (s *Set[T]) Merge(other *Set[T]) {
	for _, id := range other.order {
		s.Add(id)
	}
}

func (s *Set[T]) Copy() *Set[T] {
	return NewSet[T](s.Values()...)
}

func (s *Set[T]) IsEmpty() bool { return len(s.items) == 0 }

func (s *Set[T]) SetValues(items ...T) {
	s.Clear()
	s.Extend(items...)
}

func (s *Set[T]) Filter(fn func(T) bool) *Set[T] {
	filtered := NewSet[T]()
	for _, id := range s.order {
		if fn(id) {
			filtered.Add(id)
		}
	}
	return filtered
}

func (s *Set[T]) Values() []T {
	out := make([]T, len(s.order))
	copy(out, s.order)
	return out
}
func (s *Set[T]) Clear() {
	s.items = map[T]struct{}{}
	s.order = nil
}
func (s *Set[T]) Eq(other *Set[T]) bool {
	return s.Len() == other.Len() && s.Additions(other).Len() == 0
}
func (s *Set[T]) EqValues(ids ...T) bool {
	return s.Eq(NewSet[T](ids...))
}

func (s Set[T]) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.Values())
}

func (s *Set[T]) UnmarshalJSON(data []byte) error {
	var ids []T
	if err := json.Unmarshal(data, &ids); err != nil {
		return err
	}
	s.SetValues(ids...)
	return nil
}

func NewSet[T comparable](items ...T) *Set[T] {
	set := &Set[T]{items: map[T]struct{}{}, order: make([]T, 0, len(items))}
	for _, item := range items {
		set.Add(item)
	}
	return set
}

func NewStringSet(items ...string) *StringSet {
	return NewSet[string](items...)
}
