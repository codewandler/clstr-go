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
		Send(ctx context.Context, cmd any) error
	}
)

type handlerCtx struct {
	context.Context
	log   *slog.Logger
	send  func(ctx context.Context, cmd any) error
	sched Scheduler
}

// Schedule runs the given function asynchronously using the configured scheduler.
func (hc *handlerCtx) Schedule(f scheduleFunc) {
	hc.sched.Schedule(func() { f() })
}

func (hc *handlerCtx) Log() *slog.Logger                       { return hc.log }
func (hc *handlerCtx) Send(ctx context.Context, cmd any) error { return hc.send(ctx, cmd) }

var _ HandlerCtx = (*handlerCtx)(nil)
