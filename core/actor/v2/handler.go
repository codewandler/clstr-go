package actor

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"
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
			return nil, fmt.Errorf("no handler for msg: %T", msg)
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

func HandleMsgWithInit[IN any](
	msgHandler func(h HandlerCtx, i IN) error,
	init HandlerInitFunc,
) HandlerRegistration {
	return HandleRequestWithInit[IN, emptyOut](
		func(h HandlerCtx, i IN) (*emptyOut, error) {
			return nil, msgHandler(h, i)
		},
		init,
	)
}

func HandleEvery(interval time.Duration, msgHandler func(h HandlerCtx) error) HandlerRegistration {
	type tickMsg struct{}
	tmr := time.NewTicker(interval)
	tmr.Stop()
	start := func() {
		tmr.Reset(interval)
	}
	var sendReq func(context.Context, any) (any, error)
	go func() {

		defer tmr.Stop()
		for {
			select {
			case <-tmr.C:
				if _, err := sendReq(context.Background(), tickMsg{}); err != nil {
					slog.Default().Error("failed to send tick message", slog.Any("error", err))
				}
			}
		}
	}()
	return HandleMsgWithInit[tickMsg](
		func(h HandlerCtx, tick tickMsg) error {
			return msgHandler(h)
		},
		func(hc HandlerCtx) error {
			sendReq = hc.Request
			start()
			return nil
		},
	)
}

func HandleRequest[IN any, OUT any](h func(h HandlerCtx, i IN) (*OUT, error)) HandlerRegistration {
	return HandleRequestWithInit(h, nil)
}

func HandleRequestWithInit[IN any, OUT any](
	h func(h HandlerCtx, i IN) (*OUT, error),
	init HandlerInitFunc,
) HandlerRegistration {
	return func(registrar HandlerRegistrar) {
		mt := msgTypeFor[IN]()
		registrar.Register(
			mt,
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
			init,
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
