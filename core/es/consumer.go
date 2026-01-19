package es

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

type Checkpoint interface {
	GetLastSeq() (uint64, error)
}

type MsgCtx struct {
	ctx  context.Context
	log  *slog.Logger
	ev   Envelope
	evt  any
	live bool
}

func (c *MsgCtx) Log() *slog.Logger        { return c.log }
func (c *MsgCtx) Context() context.Context { return c.ctx }
func (c *MsgCtx) Event() any               { return c.evt }
func (c *MsgCtx) Live() bool               { return c.live }

func (c *MsgCtx) Seq() uint64           { return c.ev.Seq }
func (c *MsgCtx) Envelope() Envelope    { return c.ev }
func (c *MsgCtx) Version() Version      { return c.ev.Version }
func (c *MsgCtx) AggregateID() string   { return c.ev.AggregateID }
func (c *MsgCtx) AggregateType() string { return c.ev.AggregateType }
func (c *MsgCtx) Data() json.RawMessage { return c.ev.Data }
func (c *MsgCtx) Type() string          { return c.ev.Type }
func (c *MsgCtx) OccurredAt() time.Time { return c.ev.OccurredAt }

type Consumer struct {
	store     EventStore
	decoder   Decoder
	handler   Handler
	log       *slog.Logger
	live      chan struct{}
	isLive    atomic.Bool
	closeChan chan struct{}
	closeOnce sync.Once
	done      chan struct{}
}

func (c *Consumer) handle(ctx context.Context, ev Envelope) error {
	evt, err := c.decoder.Decode(ev)
	if err != nil {
		return fmt.Errorf("failed to decode event: %w", err)
	}
	msgCtx := MsgCtx{
		ctx: ctx,
		ev:  ev,
		evt: evt,
		log: c.log.With(
			slog.Group(
				"event",
				slog.String("id", ev.ID),
				slog.Uint64("seq", ev.Seq),
				ev.Version.SlogAttr(),
				slog.String("type", ev.Type),
				slog.String("aggregate_id", ev.AggregateID),
				slog.String("aggregate_type", ev.AggregateType),
				slog.Time("occurred_at", ev.OccurredAt),
			),
		),
	}
	if err := c.handler.Handle(msgCtx); err != nil {
		return fmt.Errorf("failed to handle event: %w", err)
	}
	return nil
}

func (c *Consumer) Start(ctx context.Context) error {

	c.log.Info("starting event consumer", slog.String("handler", fmt.Sprintf("%T", c.handler)))

	var lastSeenSeq uint64 = 0
	if cp, ok := c.handler.(Checkpoint); ok {
		var err error
		lastSeenSeq, err = cp.GetLastSeq()
		if err != nil && !errors.Is(err, ErrCheckpointNotFound) {
			return err
		}
	}

	c.log.Info("subscribing", slog.Uint64("last_seen_seq", lastSeenSeq))

	sub, err := c.store.Subscribe(
		ctx,
		WithDeliverPolicy(DeliverAllPolicy),
		WithStartSequence(lastSeenSeq+1),
	)
	if err != nil {
		return err
	}

	liveAt := sub.MaxSequence()
	if liveAt == 0 {
		c.isLive.Store(true)
		close(c.live)
	}

	go func() {
		defer func() {
			sub.Cancel()
			c.log.Info("stopped")
			close(c.done)
		}()

		for {
			select {
			case <-ctx.Done():
				return
			case <-c.closeChan:
				return

			case ev := <-sub.Chan():
				if err := c.handle(ctx, ev); err != nil {
					c.log.Error("event handler failed", slog.Any("error", err))
				}
				isLive := c.isLive.Load()
				if !isLive && ev.Seq >= liveAt {
					c.isLive.Store(true)
					close(c.live)
				}
			}
		}
	}()

	c.log.Debug("started, waiting until live")
	<-c.live
	c.log.Debug("became live")

	return nil
}

func (c *Consumer) Stop() {
	c.closeOnce.Do(func() {
		close(c.closeChan)
		<-c.done
	})
}

func NewConsumer(
	store EventStore,
	decoder Decoder,
	handler Handler,
	opts ...ConsumerOption,
) *Consumer {
	options := newConsumerOpts(opts...)
	log := options.log
	if log == nil {
		log = slog.Default()
	}
	log = log.With(slog.String("consumer", options.name))
	return &Consumer{
		log:       log,
		store:     store,
		decoder:   decoder,
		closeChan: make(chan struct{}),
		done:      make(chan struct{}),
		live:      make(chan struct{}),
		handler:   applyMiddlewares(handler, options.mws),
	}
}
