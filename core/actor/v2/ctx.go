package actor

import (
	"context"
	"log/slog"
)

type (
	HandlerCtx interface {
		context.Context
		Log() *slog.Logger
		Schedule(f scheduleFunc)
		Request(ctx context.Context, req any) (res any, err error)
		// waitScheduled is internal - waits for all scheduled tasks to complete.
		waitScheduled()
	}
)

type handlerCtx struct {
	context.Context
	log     *slog.Logger
	request func(ctx context.Context, req any) (any, error)
	sched   Scheduler
	actorID string
}

// withHandlerScope returns a new context with the actor ID embedded.
// This is used during handler execution to detect self-requests.
func (hc *handlerCtx) withHandlerScope() *handlerCtx {
	return &handlerCtx{
		Context: context.WithValue(hc.Context, actorIDKey{}, hc.actorID),
		log:     hc.log,
		request: hc.request,
		sched:   hc.sched,
		actorID: hc.actorID,
	}
}

// waitScheduled blocks until all scheduled tasks complete.
func (hc *handlerCtx) waitScheduled() {
	hc.sched.Wait()
}

// Schedule runs the given function asynchronously using the configured scheduler.
func (hc *handlerCtx) Schedule(f scheduleFunc) {
	hc.sched.Schedule(func() { f() })
}

func (hc *handlerCtx) Log() *slog.Logger                                 { return hc.log }
func (hc *handlerCtx) Request(ctx context.Context, cmd any) (any, error) { return hc.request(ctx, cmd) }

var _ HandlerCtx = (*handlerCtx)(nil)
