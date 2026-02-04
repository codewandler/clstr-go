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

	// Reply carries the result of a message handler execution.
	Reply struct {
		Result any   // Handler return value (nil for fire-and-forget)
		Error  error // Handler error, if any
	}

	// Envelope wraps a message for delivery to an actor's mailbox.
	Envelope struct {
		Type  string     // Message type name for handler dispatch
		Data  []byte     // JSON-encoded message payload
		Reply chan Reply // Channel for sending the response
	}

	// RawHandler is the low-level interface for handling actor messages.
	// Most users should use [TypedHandlers] instead of implementing this directly.
	RawHandler interface {
		// InitHandler is called once when the actor starts, before processing messages.
		InitHandler(hc HandlerCtx) error
		// HandleMessage processes a message and returns a response.
		HandleMessage(hc HandlerCtx, mt string, data []byte) (any, error)
	}

	// MsgHandlerFunc is the signature for message handler functions.
	MsgHandlerFunc func(hc HandlerCtx, msg any) (any, error)

	// HandlerInitFunc is called during actor initialization.
	HandlerInitFunc func(hc HandlerCtx) error

	// HandlerRegistrar allows registering message handlers with the actor.
	HandlerRegistrar interface {
		// Register adds a handler for a message type.
		Register(msgType string, f func() any, handle MsgHandlerFunc, init HandlerInitFunc)
	}

	// HandlerRegistration is a function that registers handlers with a registrar.
	// Create these using [HandleMsg], [HandleRequest], [HandleEvery], etc.
	HandlerRegistration func(registrar HandlerRegistrar)
)

// TypedHandlerRegistry manages message handlers for an actor, dispatching
// incoming messages to the appropriate typed handler based on message type.
type TypedHandlerRegistry struct {
	mu             sync.RWMutex
	inits          []HandlerInitFunc
	handlers       map[string]MsgHandlerFunc
	types          map[string]func() any
	defaultHandler MsgHandlerFunc
}

// ToActor creates and starts an actor using this handler registry.
func (t *TypedHandlerRegistry) ToActor(opts Options) Actor {
	return New(opts, t)
}

// Register adds a handler for a message type. This is typically called
// indirectly via [HandleMsg], [HandleRequest], etc.
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

// InitHandler initializes all registered handlers. Called by the actor on startup.
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

// HandleMessage dispatches a message to the registered handler for its type.
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

// TypedHandlers creates a new handler registry with the given handlers.
// This is the primary way to define actor message handlers.
//
// Example:
//
//	registry := actor.TypedHandlers(
//	    actor.HandleMsg[MyCommand](handleMyCommand),
//	    actor.HandleRequest[MyQuery, *MyResponse](handleMyQuery),
//	)
//	myActor := registry.ToActor(actor.Options{})
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

// DefaultHandler registers a fallback handler for messages without a specific handler.
// The message is passed as-is (not deserialized) to the handler function.
func DefaultHandler(h func(HandlerCtx, any) (any, error)) HandlerRegistration {
	return func(registrar HandlerRegistrar) {
		registrar.Register("*", func() any { return new(any) }, h, nil)
	}
}

// Init registers an initialization function called when the actor starts.
// Use this to set up state, start background goroutines, or perform other setup.
func Init(initFunc HandlerInitFunc) HandlerRegistration {
	return func(registrar HandlerRegistrar) {
		registrar.Register("", nil, nil, initFunc)
	}
}

// HandleMsg registers a fire-and-forget message handler for type IN.
// Use this for commands that don't return a value.
func HandleMsg[IN any](msgHandler func(h HandlerCtx, i IN) error) HandlerRegistration {
	return HandleRequest[IN, emptyOut](func(h HandlerCtx, i IN) (*emptyOut, error) {
		return nil, msgHandler(h, i)
	})
}

// HandleMsgWithOpts registers a message handler with additional options.
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

// HandleEvery registers a periodic task that runs at the given interval.
// The handler is called via the actor's mailbox, ensuring sequential execution
// with other handlers.
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

// HandleRequest registers a request-response handler. The handler receives
// a message of type IN and returns a response of type *OUT.
func HandleRequest[IN any, OUT any](h func(h HandlerCtx, i IN) (*OUT, error)) HandlerRegistration {
	return HandleRequestWithOpts(h)
}

// HandleOpts configures handler registration.
type HandleOpts struct {
	// MessageType overrides the default type name derived from the Go type.
	MessageType string
	// InitFunc is called during actor initialization.
	InitFunc HandlerInitFunc
}

// HandleOption configures handler registration behavior.
type HandleOption func(*HandleOpts)

// WithMessageType overrides the message type name used for routing.
// By default, the type name is derived from the Go type using reflection.
func WithMessageType(msgType string) HandleOption {
	return func(o *HandleOpts) {
		o.MessageType = msgType
	}
}

// WithInitFunc adds an initialization function to be called on actor startup.
func WithInitFunc(init HandlerInitFunc) HandleOption {
	return func(o *HandleOpts) {
		o.InitFunc = init
	}
}

// HandleRequestWithOpts registers a request-response handler with additional options.
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

// Request sends a request to an actor and waits for the response.
// The request is serialized as JSON and dispatched based on the type name of IN.
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

// Publish sends a fire-and-forget message to an actor.
// Unlike [Request], Publish does not expect a return value from the handler.
func Publish[IN any](ctx context.Context, r requester, i IN) error {
	_, err := Request[IN, emptyOut](ctx, r, i)
	return err
}

// RawRequest sends a pre-serialized message to an actor and waits for the response.
// Use [Request] for type-safe messaging.
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
