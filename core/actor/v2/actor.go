package actor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
)

type (
	OnPanic func(recovered any, stack []byte, msg any)

	Actor interface {
		Send(ctx context.Context, msg Envelope) error
		Pause() error
		Resume() error
		Step() error
		Done() <-chan struct{}
	}
)

// ---- control messages (internal) ----

type ctrlKind int

const (
	ctrlPause ctrlKind = iota
	ctrlResume
	ctrlEnableStep
	ctrlStep
	ctrlStop
)

type ctrlMsg struct {
	kind ctrlKind
}

type Options struct {
	MailboxSize int
	ControlSize int
	Context     context.Context
	Logger      *slog.Logger
	OnPanic     OnPanic
	// MaxConcurrentTasks caps the number of tasks run via HandlerCtx.Schedule.
	// If 0 or negative, scheduling is unlimited.
	MaxConcurrentTasks int
}

type BaseActor struct {
	ctx context.Context
	log *slog.Logger

	mailbox chan Envelope
	control chan ctrlMsg

	stop chan struct{}
	done chan struct{}

	mu     sync.Mutex
	closed bool

	onPanic OnPanic
}

func New(opt Options, handler RawHandler) Actor {
	if opt.MailboxSize == 0 {
		opt.MailboxSize = 1024
	}
	if opt.ControlSize == 0 {
		opt.ControlSize = 16
	}
	if opt.Context == nil {
		opt.Context = context.Background()
	}
	if opt.Logger == nil {
		opt.Logger = slog.Default()
	}
	if opt.MaxConcurrentTasks <= 0 {
		opt.MaxConcurrentTasks = 32
	}
	if opt.OnPanic == nil {
		opt.OnPanic = func(recovered any, stack []byte, msg any) {
			opt.Logger.Error("actor panicked", slog.Any("recovered", recovered), slog.Any("stack", stack), slog.Any("msg", msg))
		}
	}

	log := opt.Logger
	if log == nil {
		log = slog.Default()
	}

	ctx := opt.Context
	if ctx == nil {
		ctx = context.Background()
	}

	a := &BaseActor{
		ctx:     ctx,
		log:     log,
		mailbox: make(chan Envelope, opt.MailboxSize),
		control: make(chan ctrlMsg, opt.ControlSize),
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
		onPanic: opt.OnPanic,
	}

	// Set up scheduler used by handler context

	hCtx := &handlerCtx{
		request: func(ctx context.Context, req any) (any, error) {
			data, err := json.Marshal(req)
			if err != nil {
				return nil, err
			}

			return RawRequest(ctx, a, msgTypeOf(req), data)
		},
		log:     log,
		Context: ctx,
		sched:   NewScheduler(opt.MaxConcurrentTasks),
	}

	go a.loop(hCtx, handler)
	return a
}

// Done is closed when the actor stops.
func (a *BaseActor) Done() <-chan struct{} { return a.done }

// Stop requests shutdown and waits for completion.
func (a *BaseActor) Stop() {
	// idempotent
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		<-a.done
		return
	}
	a.closed = true
	a.mu.Unlock()

	// Try to tell the loop to stop; also close stop to unblock all sends/selects.
	select {
	case a.control <- ctrlMsg{kind: ctrlStop}:
	default:
	}
	close(a.stop)
	<-a.done
}

// Send enqueues a command (blocking until enqueued, ctx canceled, or actor stopped).
func (a *BaseActor) Send(ctx context.Context, e Envelope) error {
	if a.isClosed() {
		return errors.New("actor stopped")
	}
	select {
	case <-ctx.Done():
		return fmt.Errorf("send failed: %w", ctx.Err())
	case <-a.stop:
		return errors.New("actor stopped")
	case a.mailbox <- e:
		return nil
	}
}

// TrySend attempts a non-blocking enqueue.
func (a *BaseActor) TrySend(cmd Envelope) bool {
	if a.isClosed() {
		return false
	}
	select {
	case <-a.stop:
		return false
	case a.mailbox <- cmd:
		return true
	default:
		return false
	}
}

// Pause prevents further processing until Resume or Step.
func (a *BaseActor) Pause() error { return a.sendCtrl(ctrlPause) }

// Resume enables continuous processing (disables step mode).
func (a *BaseActor) Resume() error { return a.sendCtrl(ctrlResume) }

// EnableStepMode makes the actor process only when Step() is called.
func (a *BaseActor) EnableStepMode() error { return a.sendCtrl(ctrlEnableStep) }

// Step permits exactly one message/tick to be processed.
func (a *BaseActor) Step() error { return a.sendCtrl(ctrlStep) }

// ---- internals ----

func (a *BaseActor) isClosed() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.closed
}

func (a *BaseActor) sendCtrl(k ctrlKind) error {
	if a.isClosed() {
		return errors.New("actor stopped")
	}
	select {
	case <-a.stop:
		return errors.New("actor stopped")
	case a.control <- ctrlMsg{kind: k}:
		return nil
	}
}

func (a *BaseActor) loop(hc HandlerCtx, h RawHandler) {
	defer close(a.done)

	// execution state lives only in this goroutine
	paused := false
	stepMode := false
	permit := 1 // when >0, actor may process one message; in run mode we auto-renew

	// helper: call handler with crash containment
	safeHandle := func(mt string, data []byte) (any, error) {
		defer func() {
			if r := recover(); r != nil {
				if a.onPanic != nil {
					a.onPanic(r, debug.Stack(), nil)
				}
				// containment: keep running
			}
		}()
		return h.HandleMessage(hc, mt, data)
	}

	// helper: drain all pending control msgs (priority)
	drainControl := func() bool {
		for {
			select {
			case <-a.stop:
				return false
			case c := <-a.control:
				switch c.kind {
				case ctrlStop:
					return false
				case ctrlPause:
					paused = true
					permit = 0
				case ctrlResume:
					paused = false
					stepMode = false
					if permit == 0 {
						permit = 1
					}
				case ctrlEnableStep:
					stepMode = true
					paused = true
					permit = 0
				case ctrlStep:
					// allow exactly one processing opportunity
					permit++
				}
			default:
				return true
			}
		}
	}

	h.InitHandler(hc)

	for {
		// Always prioritize control.
		if ok := drainControl(); !ok {
			return
		}

		select {
		case <-hc.Done():
			return
		default:
		}

		// If no permit, block until a control message (or stop).
		if permit <= 0 {
			select {
			case <-a.stop:
				return
			case <-hc.Done():
				return
			case c := <-a.control:
				// process single control, then loop (drainControl next)
				switch c.kind {
				case ctrlStop:
					return
				case ctrlPause:
					paused = true
					permit = 0
				case ctrlResume:
					paused = false
					stepMode = false
					if permit == 0 {
						permit = 1
					}
				case ctrlEnableStep:
					stepMode = true
					paused = true
					permit = 0
				case ctrlStep:
					permit++
				}
			}
			continue
		}

		// With a permit, process exactly one unit of work (tick or mailbox),
		// but control can still preempt.
		var handled bool
		select {
		case <-a.stop:
			return
		case <-hc.Done():
			return
		case c := <-a.control:
			// preempt: apply control, do not consume permit yet
			switch c.kind {
			case ctrlStop:
				return
			case ctrlPause:
				paused = true
				permit = 0
			case ctrlResume:
				paused = false
				stepMode = false
				if permit == 0 {
					permit = 1
				}
			case ctrlEnableStep:
				stepMode = true
				paused = true
				permit = 0
			case ctrlStep:
				permit++
			}
			handled = false
		case msg := <-a.mailbox:
			permit--
			res, err := safeHandle(msg.Type, msg.Data)
			msg.Reply <- Reply{
				Result: res,
				Error:  err,
			}
			handled = true
		}

		// Auto-renew permit in continuous mode after successfully handling one message.
		if handled && !paused && !stepMode {
			permit++
		}
	}
}
