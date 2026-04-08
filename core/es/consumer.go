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
//
// MsgCtx is passed by value to Handler.Handle, so each invocation owns an
// independent copy. This makes the lazy Log() cache safe without a mutex:
// the log field is only ever written by the single goroutine that holds the
// value, so no synchronisation is required.
type MsgCtx struct {
	ctx     context.Context
	baseLog *slog.Logger // consumer-level logger; event attrs added lazily
	log     *slog.Logger // nil until Log() is first called
	ev      Envelope
	evt     any
	live    bool
}

// Log returns a logger annotated with the current event's fields.
// The annotated logger is constructed lazily on first call to avoid
// a heap allocation per event when the handler never calls Log().
func (c *MsgCtx) Log() *slog.Logger {
	if c.log == nil {
		c.log = c.baseLog.With(
			slog.Group(
				"event",
				slog.String("id", c.ev.ID),
				slog.Uint64("seq", c.ev.Seq),
				c.ev.Version.SlogAttr(),
				slog.String("type", c.ev.Type),
				slog.String("aggregate_id", c.ev.AggregateID),
				slog.String("aggregate_type", c.ev.AggregateType),
				slog.Time("occurred_at", c.ev.OccurredAt),
			),
		)
	}
	return c.log
}
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
//
// Consumer is self-healing: if the underlying subscription fails (e.g. NATS server
// restart, network partition recovery), it automatically re-subscribes with
// exponential backoff rather than stopping the process.
//
// A Consumer is single-use: Start() may be called at most once. Subsequent
// concurrent calls share the same goroutine and return the same result. After
// Stop() returns, the Consumer cannot be restarted — create a new one with
// NewConsumer.
type Consumer struct {
	store                   EventStore
	decoder                 Decoder
	handler                 Handler
	log                     *slog.Logger
	live                    chan struct{}
	liveOnce                sync.Once
	isLive                  atomic.Bool
	closeChan               chan struct{}
	closeOnce               sync.Once
	startOnce               sync.Once
	startErr                error // set inside startOnce.Do; safe to read after Do returns
	done                    chan struct{}
	died                    chan struct{}
	shutdownTimeout         time.Duration
	reconnectBackoffInitial time.Duration
	reconnectBackoffMax     time.Duration
	name                    string
	metrics                 ESMetrics
	errorStrategy           ErrorStrategy
}

// Died returns a channel that is closed when the consumer goroutine exits for
// any reason — normal Stop(), context cancellation, or a failed Start().
// It is a signal-only channel: no value is ever sent, it is only closed.
// Callers can use it to observe consumer lifecycle completion.
func (c *Consumer) Died() <-chan struct{} { return c.died }

// readCheckpoint returns the last successfully processed sequence number.
// Returns 0 if no checkpoint exists or the handler does not implement Checkpoint.
func (c *Consumer) readCheckpoint() uint64 {
	cp, ok := c.handler.(Checkpoint)
	if !ok {
		return 0
	}
	seq, err := cp.GetLastSeq()
	if err != nil && !errors.Is(err, ErrCheckpointNotFound) {
		c.log.Error("failed to read checkpoint, starting from beginning", slog.Any("error", err))
		return 0
	}
	return seq
}

// wait sleeps for d, returning false early if ctx is cancelled or Stop() is called.
// Uses time.NewTimer (with explicit Stop) rather than time.After to avoid leaking
// the timer goroutine when ctx or closeChan fires before the deadline.
func (c *Consumer) wait(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	case <-c.closeChan:
		return false
	}
}

func (c *Consumer) handle(ctx context.Context, ev Envelope) error {
	live := c.isLive.Load()

	// instrument
	defer c.metrics.ConsumerEventDuration(ev.Type, live).ObserveDuration()

	evt, err := c.decoder.Decode(ev)
	if err != nil {
		c.metrics.ConsumerEventProcessed(ev.Type, live, false)
		return &DecodeError{Cause: err}
	}
	// baseLog is set here; the event-annotated logger is built lazily in
	// MsgCtx.Log() only when the handler actually calls it.
	msgCtx := MsgCtx{
		ctx:     ctx,
		ev:      ev,
		evt:     evt,
		live:    live,
		baseLog: c.log,
	}
	if err := c.handler.Handle(msgCtx); err != nil {
		c.metrics.ConsumerEventProcessed(ev.Type, live, false)
		return fmt.Errorf("failed to handle event: %w", err)
	}
	c.metrics.ConsumerEventProcessed(ev.Type, live, true)
	return nil
}

// DecodeError signals that the event decoder (registry) could not decode
// an event. Retrying is pointless because the event payload will not change.
// The consumer always skips decode errors regardless of ErrorStrategy.
type DecodeError struct {
	Cause error
}

func (e *DecodeError) Error() string { return fmt.Sprintf("failed to decode event: %s", e.Cause) }
func (e *DecodeError) Unwrap() error { return e.Cause }

// runSubscription runs the inner event-processing select loop for a single
// subscription. Returns nil on clean exit (ctx cancel, Stop()), or a non-nil
// error when the subscription itself failed and should be retried.
func (c *Consumer) runSubscription(ctx context.Context, sub Subscription, liveAt uint64) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-c.closeChan:
			return nil

		case subErr, ok := <-sub.Done():
			if !ok {
				// Clean shutdown signalled via Done channel.
				return nil
			}
			return subErr // subscription failed — caller will retry

		case ev := <-sub.Chan():
			// Defensive guard: zero-value Envelope means channel was closed without
			// a Done() signal (non-compliant Subscription impl or ordering race).
			if ev.ID == "" && ev.Type == "" {
				return errors.New("subscription channel closed unexpectedly (no Done signal)")
			}
			if err := c.handle(ctx, ev); err != nil {
				// Decode errors can never be fixed by retrying — always skip.
				var decErr *DecodeError
				if errors.As(err, &decErr) {
					c.log.Error("event decode failed, skipping",
						slog.Any("error", err),
						slog.Uint64("seq", ev.Seq),
						slog.String("event_type", ev.Type))
				} else {
					// Handler error — follow the configured strategy.
					switch c.errorStrategy {
					case ErrorStrategyStop:
						c.log.Error("event handler failed, stopping subscription for retry",
							slog.Any("error", err),
							slog.Uint64("seq", ev.Seq),
							slog.String("event_type", ev.Type))
						return fmt.Errorf("handler error at seq %d: %w", ev.Seq, err)
					case ErrorStrategySkip:
						c.log.Error("event handler failed, skipping event",
							slog.Any("error", err),
							slog.Uint64("seq", ev.Seq),
							slog.String("event_type", ev.Type))
					}
				}
			}
			if !c.isLive.Load() && ev.Seq >= liveAt {
				c.isLive.Store(true)
				c.liveOnce.Do(func() { close(c.live) })
			}
			// report consumer lag
			if liveAt > ev.Seq {
				c.metrics.ConsumerLag(c.name, int64(liveAt-ev.Seq))
			} else {
				c.metrics.ConsumerLag(c.name, 0)
			}
		}
	}
}

// loop is the self-healing subscription retry loop. It runs in a dedicated
// goroutine, re-subscribing with exponential backoff whenever the subscription
// fails. It exits cleanly when ctx is cancelled or Stop() is called.
func (c *Consumer) loop(ctx context.Context) {
	defer func() {
		if lc, ok := c.handler.(HandlerLifecycleShutdown); ok {
			// Use context.Background() so the shutdown timeout is not already
			// expired when ctx was cancelled (e.g. during env shutdown).
			shutdownCtx, cancel := context.WithTimeout(context.Background(), c.shutdownTimeout)
			defer cancel()
			if err := lc.Shutdown(shutdownCtx); err != nil {
				c.log.Error("failed to shutdown consumer lifecycle", slog.Any("error", err))
			}
		}
		c.log.Info("stopped")
		close(c.done)
		close(c.died) // always clean — consumer self-heals, never gives up
	}()

	backoff := c.reconnectBackoffInitial

	for {
		// Fast exit check before each attempt.
		select {
		case <-ctx.Done():
			return
		case <-c.closeChan:
			return
		default:
		}

		lastSeenSeq := c.readCheckpoint()

		sub, err := c.store.Subscribe(
			ctx,
			WithDeliverPolicy(DeliverAllPolicy),
			WithStartSequence(lastSeenSeq+1),
		)
		if err != nil {
			c.metrics.ConsumerReconnects(c.name)
			c.log.Error("failed to subscribe, retrying",
				slog.Any("error", err),
				slog.Duration("backoff", backoff))
			if !c.wait(ctx, backoff) {
				return
			}
			backoff = min(backoff*2, c.reconnectBackoffMax)
			continue
		}
		backoff = c.reconnectBackoffInitial // reset on successful subscribe

		// Mark live if already caught up.
		liveAt := sub.MaxSequence()
		if !c.isLive.Load() && (liveAt == 0 || liveAt == lastSeenSeq) {
			c.isLive.Store(true)
			c.liveOnce.Do(func() { close(c.live) })
		}

		subErr := c.runSubscription(ctx, sub, liveAt)
		sub.Cancel()

		if subErr == nil {
			return // clean exit (ctx cancel or Stop())
		}

		// Subscription failed — increment metric, log, back off, retry.
		c.metrics.ConsumerReconnects(c.name)
		c.log.Error("subscription failed, will retry",
			slog.Any("error", subErr),
			slog.Duration("backoff", backoff))
		if !c.wait(ctx, backoff) {
			return
		}
		backoff = min(backoff*2, c.reconnectBackoffMax)
	}
}

func (c *Consumer) Start(ctx context.Context) error {
	c.log.Info("starting event consumer", slog.String("handler", fmt.Sprintf("%T", c.handler)))

	// startOnce protects both HandlerLifecycleStart and goroutine launch.
	// sync.Once provides a happens-before guarantee: all callers that reach
	// this point after Do() has completed can safely read c.startErr.
	c.startOnce.Do(func() {
		if lc, ok := c.handler.(HandlerLifecycleStart); ok {
			if err := lc.Start(ctx); err != nil {
				c.startErr = fmt.Errorf("failed to start consumer lifecycle: %w", err)
				// Close done and died so that Stop() and any Died() readers
				// are not left blocked when the loop was never launched.
				close(c.done)
				close(c.died)
				return
			}
			c.log.Debug("handler started")
		}
		go c.loop(ctx)
	})

	if c.startErr != nil {
		return c.startErr
	}

	c.log.Debug("started, waiting until live")
	// Prioritise c.live: if the consumer went live and the loop exited in the
	// same scheduler cycle, the non-deterministic select below might pick
	// c.done. Check c.live first with a fast non-blocking pass.
	select {
	case <-c.live:
		c.log.Debug("became live")
		return nil
	default:
	}
	select {
	case <-c.live:
		c.log.Debug("became live")
		return nil
	case <-c.done:
		// loop exited before going live (only on ctx cancel or Stop())
		// Re-check live in case it closed between the two selects.
		select {
		case <-c.live:
			c.log.Debug("became live")
			return nil
		default:
		}
		return fmt.Errorf("consumer stopped before becoming live")
	case <-ctx.Done():
		return ctx.Err()
	}
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
		log:       log,
		store:     store,
		decoder:   decoder,
		closeChan: make(chan struct{}),
		done:      make(chan struct{}),
		live:      make(chan struct{}),
		// died is unbuffered: it is only ever closed (never sent to), so
		// capacity would be wasted. close() on an unbuffered channel broadcasts
		// to all current and future readers.
		died:                    make(chan struct{}),
		handler:                 applyMiddlewares(handler, options.mws),
		shutdownTimeout:         options.shutdownTimeout,
		reconnectBackoffInitial: options.reconnectBackoffInitial,
		reconnectBackoffMax:     options.reconnectBackoffMax,
		name:                    options.name,
		metrics:                 metrics,
		errorStrategy:           options.errorStrategy,
	}
}
