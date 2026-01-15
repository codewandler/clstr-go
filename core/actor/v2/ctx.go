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
	}
)

type handlerCtx struct {
	context.Context
	log     *slog.Logger
	request func(ctx context.Context, req any) (any, error)
	sched   Scheduler
}

// Schedule runs the given function asynchronously using the configured scheduler.
func (hc *handlerCtx) Schedule(f scheduleFunc) {
	hc.sched.Schedule(func() { f() })
}

func (hc *handlerCtx) Log() *slog.Logger                                 { return hc.log }
func (hc *handlerCtx) Request(ctx context.Context, cmd any) (any, error) { return hc.request(ctx, cmd) }

var _ HandlerCtx = (*handlerCtx)(nil)
