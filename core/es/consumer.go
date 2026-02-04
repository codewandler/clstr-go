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

// Checkpoint is implemented by handlers that track their processing progress.
// When a handler implements this interface, the Consumer will use GetLastSeq()
// to determine where to resume processing after a restart.
type Checkpoint interface {
	// GetLastSeq returns the sequence number of the last successfully processed event.
	// Returns ErrCheckpointNotFound if no checkpoint exists.
	GetLastSeq() (uint64, error)
}

// MsgCtx provides context for handling a single event. It wraps the event
// envelope with additional metadata about the processing context, including
// whether the consumer is in live mode (processing real-time events) or
// catching up on historical events.
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

// Consumer processes events from an EventStore by subscribing to the stream
// and dispatching events to a Handler. It supports checkpointing for exactly-once
// processing semantics and live mode detection for distinguishing between
// historical replay and real-time events.
type Consumer struct {
	store           EventStore
	decoder         Decoder
	handler         Handler
	log             *slog.Logger
	live            chan struct{}
	isLive          atomic.Bool
	closeChan       chan struct{}
	closeOnce       sync.Once
	done            chan struct{}
	shutdownTimeout time.Duration
	name            string
	metrics         ESMetrics
}

func (c *Consumer) handle(ctx context.Context, ev Envelope) error {
	live := c.isLive.Load()

	// instrument
	defer c.metrics.ConsumerEventDuration(ev.Type, live).ObserveDuration()

	evt, err := c.decoder.Decode(ev)
	if err != nil {
		c.metrics.ConsumerEventProcessed(ev.Type, live, false)
		return fmt.Errorf("failed to decode event: %w", err)
	}
	msgCtx := MsgCtx{
		ctx:  ctx,
		ev:   ev,
		evt:  evt,
		live: live,
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
		c.metrics.ConsumerEventProcessed(ev.Type, live, false)
		return fmt.Errorf("failed to handle event: %w", err)
	}
	c.metrics.ConsumerEventProcessed(ev.Type, live, true)
	return nil
}

func (c *Consumer) Start(ctx context.Context) error {
	c.log.Info("starting event consumer", slog.String("handler", fmt.Sprintf("%T", c.handler)))

	if lc, ok := c.handler.(HandlerLifecycleStart); ok {
		if err := lc.Start(ctx); err != nil {
			return fmt.Errorf("failed to start consumer lifecycle: %w", err)
		}
		c.log.Debug("handler started")
	}

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
	if liveAt == 0 || liveAt == lastSeenSeq {
		c.isLive.Store(true)
		close(c.live)
	}

	go func() {
		defer func() {
			sub.Cancel()
			if lc, ok := c.handler.(HandlerLifecycleShutdown); ok {
				shutdownCtx, cancel := context.WithTimeout(ctx, c.shutdownTimeout)
				defer cancel()
				if err := lc.Shutdown(shutdownCtx); err != nil {
					c.log.Error("failed to shutdown consumer lifecycle", slog.Any("error", err))
				}
			}
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
				// report consumer lag
				if liveAt > ev.Seq {
					c.metrics.ConsumerLag(c.name, int64(liveAt-ev.Seq))
				} else {
					c.metrics.ConsumerLag(c.name, 0)
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

	metrics := options.metrics
	if metrics == nil {
		metrics = NopESMetrics()
	}

	return &Consumer{
		log:             log,
		store:           store,
		decoder:         decoder,
		closeChan:       make(chan struct{}),
		done:            make(chan struct{}),
		live:            make(chan struct{}),
		handler:         applyMiddlewares(handler, options.mws),
		shutdownTimeout: options.shutdownTimeout,
		name:            options.name,
		metrics:         metrics,
	}
}
