package actor

import (
	"context"
	"encoding/json"
)

type (
	StateOp[T any]            func(*T)
	StateReadOp[T any, R any] func(*T) R

	StateTask[T any] interface {
		run(*T, func(*T))
	}

	State[T any] struct {
		ctx   context.Context
		data  *T
		tasks chan StateTask[T]
		cb    func(*T)
	}
)

type writeTask[T any] struct {
	ops  []StateOp[T]
	done chan struct{}
}

//lint:ignore U1000 implements StateTask[T] interface, called via State.run()
func (t writeTask[T]) run(st *T, cb func(*T)) {
	for _, op := range t.ops {
		op(st)
	}
	if cb != nil {
		cb(st)
	}
	if t.done != nil {
		close(t.done)
	}
}

type readTask[T any] struct {
	runFunc func(*T)
}

//lint:ignore U1000 implements StateTask[T] interface, called via State.run()
func (t readTask[T]) run(st *T, _ func(*T)) { t.runFunc(st) }

func NewState[T any](ctx context.Context, data *T, cb func(*T)) *State[T] {
	i := &State[T]{
		ctx:   ctx,
		tasks: make(chan StateTask[T], 1),
		data:  data,
		cb:    cb,
	}
	go i.run(ctx)
	return i
}

func (s *State[T]) MarshalJSON() ([]byte, error) {
	type dataErr struct {
		data []byte
		err  error
	}
	v := Read[T, dataErr](s, func(st *T) dataErr {
		d, err := json.Marshal(s.data)
		return dataErr{d, err}
	})
	return v.data, v.err
}

func (s *State[T]) UnmarshalJSON(data []byte) error {
	errChan := make(chan error, 1)
	s.Process(func(t *T) { errChan <- json.Unmarshal(data, t) })
	return <-errChan

}

func (s *State[T]) Process(ops ...StateOp[T]) {
	done := make(chan struct{})
	s.tasks <- writeTask[T]{ops: ops, done: done}
	<-done
}

func (s *State[T]) Submit(ops ...StateOp[T]) <-chan struct{} {
	done := make(chan struct{})
	s.tasks <- writeTask[T]{ops: ops, done: done}
	return done
}

// Read blocks and returns R
func Read[T any, R any](s *State[T], op func(*T) R) R {
	return <-ReadAsync[T, R](s, op)
}

// ReadAsync is non-blocking: returns a future chan R immediately
func ReadAsync[T any, R any](s *State[T], op func(*T) R) <-chan R {
	out := make(chan R, 1)

	// enqueue a read task
	s.tasks <- readTask[T]{
		runFunc: func(st *T) {
			out <- op(st)
			close(out)
		},
	}

	return out
}

func (s *State[T]) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case t := <-s.tasks:
			t.run(s.data, s.cb)
		}
	}
}
