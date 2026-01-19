package es

import (
	"context"
	"log/slog"
	"time"
)

type (
	Handler interface {
		Handle(msgCtx MsgCtx) error
	}
	HandlerLifecycle interface {
		Start(ctx context.Context) error
		Shutdown(ctx context.Context) error
	}
	HandleFunc           func(ctx MsgCtx) error
	HandlerMiddleware    func(next Handler) Handler
	MiddlewareHandleFunc func(ctx MsgCtx, next Handler) error
)

func applyMiddlewares(h Handler, middlewares []HandlerMiddleware) Handler {
	for i := len(middlewares) - 1; i >= 0; i-- {
		h = middlewares[i](h)
	}
	return h
}

// === handler func ===

func (f HandleFunc) Handle(ctx MsgCtx) error { return f(ctx) }
func Handle(f HandleFunc) HandleFunc         { return f }

// === middleware ===

type middleware struct {
	next Handler
	mw   MiddlewareHandleFunc
}

func (m *middleware) Handle(msgCtx MsgCtx) error { return m.mw(msgCtx, m.next) }

func MiddlewareHandle(mw MiddlewareHandleFunc) HandlerMiddleware {
	return func(next Handler) Handler {
		return &middleware{
			next: next,
			mw:   mw,
		}
	}
}

// === log ===

func NewLogMiddleware(attrs ...any) HandlerMiddleware {
	return MiddlewareHandle(func(ctx MsgCtx, next Handler) (err error) {
		handleAt := time.Now()

		log := ctx.Log().With(attrs...)

		err = next.Handle(ctx)
		if err != nil {
			log.Error("failed", slog.Any("error", err), slog.Duration("duration", time.Since(handleAt)))
		} else {
			log.Debug("handled", slog.Duration("duration", time.Since(handleAt)))
		}

		return err
	})
}

// === checkpoint middleware ===

type checkpointHandler struct {
	cp CpStore
	h  Handler
}

func (c *checkpointHandler) GetLastSeq() (uint64, error) { return c.cp.Get() }

func (c *checkpointHandler) Handle(msgCtx MsgCtx) (err error) {
	lastSeenSeq, err := c.cp.Get()
	if err != nil {
		return err
	}

	minSeq := lastSeenSeq + 1

	if msgCtx.Seq() < minSeq {
		msgCtx.log.Debug("skip", slog.Uint64("min_seq", minSeq), slog.String("middleware", "checkpoint"))
		return nil
	}

	err = c.h.Handle(msgCtx)
	if err != nil {
		return err
	}

	err = c.cp.Set(msgCtx.Seq())
	if err != nil {
		return err
	}

	return nil
}

var _ Handler = (*checkpointHandler)(nil)

func NewCheckpointMiddleware(cp CpStore) HandlerMiddleware {
	return func(handler Handler) Handler {
		return &checkpointHandler{cp: cp, h: handler}
	}
}
