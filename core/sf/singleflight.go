package sf

import "golang.org/x/sync/singleflight"

type Singleflight[T any] struct {
	group singleflight.Group
}

func (s *Singleflight[T]) Do(key string, fn func() (*T, error)) (*T, error) {
	v, err, _ := s.group.Do(key, func() (out any, err error) {
		return fn()
	})
	if err != nil {
		return nil, err
	}
	return v.(*T), nil
}

func New[T any]() *Singleflight[T] {
	return &Singleflight[T]{}
}
