package actor

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	gonanoid "github.com/matoous/go-nanoid/v2"
)

type (
	emptyOut struct{}

	Reply struct {
		Result any
		Error  error
	}

	Envelope struct {
		Type  string
		Data  []byte
		Reply chan Reply
	}

	RawHandler interface {
		InitHandler(hc HandlerCtx) error
		HandleMessage(hc HandlerCtx, mt string, data []byte) (any, error)
	}

	MsgHandlerFunc  func(hc HandlerCtx, msg any) (any, error)
	HandlerInitFunc func(hc HandlerCtx) error

	HandlerRegistrar interface {
		Register(msgType string, f func() any, handle MsgHandlerFunc, init HandlerInitFunc)
	}

	HandlerRegistration func(registrar HandlerRegistrar)
)

type TypedHandlerRegistry struct {
	mu             sync.RWMutex
	inits          []HandlerInitFunc
	handlers       map[string]MsgHandlerFunc
	types          map[string]func() any
	defaultHandler MsgHandlerFunc
}

func (t *TypedHandlerRegistry) ToActor(opts Options) Actor {
	return New(opts, t)
}

func (t *TypedHandlerRegistry) Register(msgType string, typeFactory func() any, msgHandler MsgHandlerFunc, init HandlerInitFunc) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// TODO: use struct

	if msgType != "" {
		if msgHandler != nil {
			t.handlers[msgType] = msgHandler
		}

		if typeFactory != nil {
			t.types[msgType] = typeFactory
		}
	}

	if init != nil {
		t.inits = append(t.inits, init)
	}

}

func (t *TypedHandlerRegistry) InitHandler(hc HandlerCtx) error {
	// store default handler
	dh, ok := t.handlers["*"]
	if ok {
		t.defaultHandler = dh
	} else {
		t.defaultHandler = func(hc HandlerCtx, msg any) (any, error) {
			mt := msgTypeOf(msg)
			return nil, fmt.Errorf("no handler for msg: msg_type=%s go_type=%T msg=%+v", mt, msg, msg)
		}
	}

	// call all init funcs
	for _, i := range t.inits {
		if err := i(hc); err != nil {
			return fmt.Errorf("failed to init handler: %w", err)
		}
	}

	return nil
}

func (t *TypedHandlerRegistry) HandleMessage(hc HandlerCtx, mt string, data []byte) (any, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	h, ok := t.handlers[mt]
	if !ok {
		return t.defaultHandler(hc, data)
	}

	f, ok := t.types[mt]
	if !ok {
		return nil, fmt.Errorf("no type registered for message type %s", mt)
	}
	tt := f()
	err := json.Unmarshal(data, &tt)
	if err != nil {
		return nil, err
	}
	return h(hc, tt)
}

func TypedHandlers(handlers ...HandlerRegistration) *TypedHandlerRegistry {
	th := &TypedHandlerRegistry{
		handlers: make(map[string]MsgHandlerFunc),
		types:    make(map[string]func() any),
		inits:    make([]HandlerInitFunc, 0),
	}

	for _, h := range handlers {
		h(th)
	}

	return th
}

func DefaultHandler(h func(HandlerCtx, any) (any, error)) HandlerRegistration {
	return func(registrar HandlerRegistrar) {
		registrar.Register("*", func() any { return new(any) }, h, nil)
	}
}

func Init(initFunc HandlerInitFunc) HandlerRegistration {
	return func(registrar HandlerRegistrar) {
		registrar.Register("", nil, nil, initFunc)
	}
}

func HandleMsg[IN any](msgHandler func(h HandlerCtx, i IN) error) HandlerRegistration {
	return HandleRequest[IN, emptyOut](func(h HandlerCtx, i IN) (*emptyOut, error) {
		return nil, msgHandler(h, i)
	})
}

func HandleMsgWithOpts[IN any](
	msgHandler func(h HandlerCtx, i IN) error,
	opts ...HandleOption,
) HandlerRegistration {
	return HandleRequestWithOpts[IN, emptyOut](
		func(h HandlerCtx, i IN) (*emptyOut, error) {
			return nil, msgHandler(h, i)
		},
		opts...,
	)
}

type tickMsg struct{ mt string }

func (m tickMsg) MsgType() string { return m.mt }

func HandleEvery(interval time.Duration, msgHandler func(h HandlerCtx) error) HandlerRegistration {
	msg := tickMsg{mt: "tick/" + gonanoid.Must()}

	return HandleMsgWithOpts[tickMsg](
		func(h HandlerCtx, tick tickMsg) error {
			return msgHandler(h)
		},
		WithMessageType(msg.MsgType()),
		WithInitFunc(func(hc HandlerCtx) error {
			tmr := time.NewTicker(interval)
			go func() {
				defer tmr.Stop()
				for {
					select {
					case <-hc.Done():
						return
					case <-tmr.C:
						if _, err := hc.Request(hc, msg); err != nil {
							slog.Default().Warn("failed to send tick message", slog.Any("error", err))
						}
					}
				}
			}()
			return nil
		}),
	)
}

func HandleRequest[IN any, OUT any](h func(h HandlerCtx, i IN) (*OUT, error)) HandlerRegistration {
	return HandleRequestWithOpts(h)
}

type HandleOpts struct {
	MessageType string
	InitFunc    HandlerInitFunc
}

type HandleOption func(*HandleOpts)

func WithMessageType(msgType string) HandleOption {
	return func(o *HandleOpts) {
		o.MessageType = msgType
	}
}

func WithInitFunc(init HandlerInitFunc) HandleOption {
	return func(o *HandleOpts) {
		o.InitFunc = init
	}
}

func HandleRequestWithOpts[IN any, OUT any](
	h func(h HandlerCtx, i IN) (*OUT, error),
	opts ...HandleOption,
) HandlerRegistration {
	handleOpts := HandleOpts{
		MessageType: msgTypeFor[IN](),
	}
	for _, opt := range opts {
		opt(&handleOpts)
	}
	return func(registrar HandlerRegistrar) {
		registrar.Register(
			handleOpts.MessageType,
			func() any { return new(IN) },
			func(hc HandlerCtx, msg any) (any, error) {
				// TODO: validate

				i, ok := msg.(*IN)
				if !ok {
					return nil, fmt.Errorf("invalid request message type: %T", msg)
				}
				out, err := h(hc, *i)
				if err != nil {
					return nil, err
				}
				return out, nil
			},
			handleOpts.InitFunc,
		)
	}
}

type requester interface {
	Send(ctx context.Context, msg Envelope) error
}

func Request[IN any, OUT any](ctx context.Context, r requester, i IN) (*OUT, error) {
	data, err := json.Marshal(i)
	if err != nil {
		return nil, err
	}
	res, err := RawRequest(ctx, r, msgTypeFor[IN](), data)
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, nil
	}
	return res.(*OUT), nil
}

func Publish[IN any](ctx context.Context, r requester, i IN) error {
	_, err := Request[IN, emptyOut](ctx, r, i)
	return err
}

func RawRequest(ctx context.Context, r requester, msgType string, data []byte) (any, error) {

	replyChan := make(chan Reply, 1)

	err := r.Send(ctx, Envelope{Type: msgType, Data: data, Reply: replyChan})
	if err != nil {
		return nil, err
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case reply := <-replyChan:
		return reply.Result, reply.Error
	}
}
