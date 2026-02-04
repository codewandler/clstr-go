package sf

import "golang.org/x/sync/singleflight"

// Singleflight deduplicates concurrent function calls with the same key.
// Only the first caller executes the function; others wait and receive
// the same result.
type Singleflight[T any] struct {
	group singleflight.Group
}

// Do executes fn for the given key, deduplicating concurrent calls.
// If a call is already in-flight for this key, Do blocks until it completes
// and returns the same result. The function fn is guaranteed to execute
// at most once per key at any given time.
func (s *Singleflight[T]) Do(key string, fn func() (*T, error)) (*T, error) {
	v, err, _ := s.group.Do(key, func() (out any, err error) {
		return fn()
	})
	if err != nil {
		return nil, err
	}
	return v.(*T), nil
}

// New creates a new Singleflight instance for type T.
func New[T any]() *Singleflight[T] {
	return &Singleflight[T]{}
}
