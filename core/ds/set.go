// Package ds provides generic data structures for use in event sourcing systems.
package ds

import (
	"encoding/json"
	"fmt"
)

type StringSet = Set[string]

// Set is an ordered set that maintains both O(1) membership testing and
// insertion order preservation. This is useful for deterministic iteration
// in event sourcing scenarios.
//
// # Mutation Semantics
//
// The following methods mutate the receiver:
//   - Add, Extend, Remove, Merge, Clear, SetValues
//
// The following methods return new sets without modifying the receiver:
//   - Filter, Copy, Intersect, Additions, Removals, Diff, Values
type Set[T comparable] struct {
	items map[T]struct{}
	order []T // preserves insertion order
}

func (s *Set[T]) String() string {
	return fmt.Sprintf("%v", s.order)
}

// Add adds the given id to the set. No-op if already present. (mutates)
func (s *Set[T]) Add(id T) {
	if s.contains(id) {
		return
	}
	s.items[id] = struct{}{}
	s.order = append(s.order, id)
}

// Extend adds all given ids to the set and returns a new set containing
// only the elements that were actually added (i.e., not already present). (mutates)
func (s *Set[T]) Extend(ids ...T) *Set[T] {
	additions := s.Additions(NewSet(ids...))
	for _, id := range additions.Values() {
		s.Add(id)
	}
	return additions
}

// Len returns the number of elements in the set.
func (s *Set[T]) Len() int { return len(s.items) }

// Remove removes the given ids from the set. (mutates)
// This operation is O(n) where n is the set size.
func (s *Set[T]) Remove(ids ...T) {
	if len(ids) == 0 {
		return
	}

	// Build removal set for O(1) lookup
	toRemove := make(map[T]struct{}, len(ids))
	for _, id := range ids {
		if _, ok := s.items[id]; ok {
			toRemove[id] = struct{}{}
			delete(s.items, id)
		}
	}

	if len(toRemove) == 0 {
		return
	}

	// Rebuild order slice, filtering out removed elements - O(n)
	newOrder := make([]T, 0, len(s.order)-len(toRemove))
	for _, v := range s.order {
		if _, removed := toRemove[v]; !removed {
			newOrder = append(newOrder, v)
		}
	}
	s.order = newOrder
}

// ContainsValues returns true if all given ids are present in the set.
func (s *Set[T]) ContainsValues(ids ...T) bool {
	return s.ContainsAll(NewSet(ids...))
}

// ContainsAny returns true if at least one element of other is present in s.
func (s *Set[T]) ContainsAny(other *Set[T]) bool {
	for _, id := range other.order {
		if s.contains(id) {
			return true
		}
	}
	return false
}

// ContainsAll returns true if all elements of other are present in s.
func (s *Set[T]) ContainsAll(other *Set[T]) bool {
	for id := range other.items {
		if !s.contains(id) {
			return false
		}
	}
	return true
}

// Contains returns true if v is present in the set.
func (s *Set[T]) Contains(v T) bool {
	return s.contains(v)
}

func (s *Set[T]) contains(id T) bool {
	_, ok := s.items[id]
	return ok
}

// IsSubsetOf returns true if all elements of s are contained in other.
// An empty set is a subset of any set.
func (s *Set[T]) IsSubsetOf(other *Set[T]) bool {
	return other.ContainsAll(s)
}

// IsSupersetOf returns true if s contains all elements of other.
// Any set is a superset of the empty set.
func (s *Set[T]) IsSupersetOf(other *Set[T]) bool {
	return s.ContainsAll(other)
}

// ForEach iterates over all elements in insertion order, calling fn for each.
// This is more efficient than Values() when you don't need a slice copy.
func (s *Set[T]) ForEach(fn func(T)) {
	for _, id := range s.order {
		fn(id)
	}
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

// Removals returns a new set that contains all elements that are present in
// the receiver (s) but not in other. In other words: the elements you need
// to remove from s to obtain other. The order follows the receiver's insertion order.
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

// Merge adds all elements from other to s. (mutates)
func (s *Set[T]) Merge(other *Set[T]) {
	for _, id := range other.order {
		s.Add(id)
	}
}

// Copy returns a new set with the same elements and order.
func (s *Set[T]) Copy() *Set[T] {
	return NewSet(s.Values()...)
}

// IsEmpty returns true if the set contains no elements.
func (s *Set[T]) IsEmpty() bool { return len(s.items) == 0 }

// SetValues replaces all elements with the given items. (mutates)
func (s *Set[T]) SetValues(items ...T) {
	s.Clear()
	s.Extend(items...)
}

// Filter returns a new set containing only elements for which fn returns true.
// The order of the resulting set preserves the receiver's insertion order.
func (s *Set[T]) Filter(fn func(T) bool) *Set[T] {
	filtered := NewSet[T]()
	for _, id := range s.order {
		if fn(id) {
			filtered.Add(id)
		}
	}
	return filtered
}

// Values returns a copy of the elements in insertion order.
func (s *Set[T]) Values() []T {
	out := make([]T, len(s.order))
	copy(out, s.order)
	return out
}

// Clear removes all elements from the set. (mutates)
func (s *Set[T]) Clear() {
	s.items = map[T]struct{}{}
	s.order = nil
}

// Eq returns true if both sets contain the same elements (order is ignored).
func (s *Set[T]) Eq(other *Set[T]) bool {
	return s.Len() == other.Len() && s.Additions(other).Len() == 0
}

// EqValues returns true if the set contains exactly the given ids.
func (s *Set[T]) EqValues(ids ...T) bool {
	return s.Eq(NewSet(ids...))
}

// MarshalJSON serializes the set as an ordered JSON array.
func (s Set[T]) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.Values())
}

// UnmarshalJSON deserializes a JSON array into the set.
func (s *Set[T]) UnmarshalJSON(data []byte) error {
	var ids []T
	if err := json.Unmarshal(data, &ids); err != nil {
		return err
	}
	s.SetValues(ids...)
	return nil
}

// NewSet creates a new set with the given items.
func NewSet[T comparable](items ...T) *Set[T] {
	set := &Set[T]{items: map[T]struct{}{}, order: make([]T, 0, len(items))}
	for _, item := range items {
		set.Add(item)
	}
	return set
}

// NewStringSet creates a new string set with the given items.
func NewStringSet(items ...string) *StringSet {
	return NewSet(items...)
}
