package cluster

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

const (
	DefaultMaxConcurrentHandlers = 100
	DefaultHandlerTimeout        = 30 * time.Second
)

// responseFrame is the minimal response encoding for Request().
// Transport remains backend-agnostic because it's just bytes on the wire.
type responseFrame struct {
	Data []byte `json:"data,omitempty"`
	Err  string `json:"err,omitempty"`
}

type handlerFn func(context.Context, Envelope) ([]byte, error)

// MemoryTransportOpts configures the in-memory transport.
type MemoryTransportOpts struct {
	// MaxConcurrentHandlers limits concurrent handler goroutines.
	// 0 = default (100), -1 = unlimited
	MaxConcurrentHandlers int

	// HandlerTimeout is the maximum duration for a handler to complete.
	// 0 = default (30s)
	HandlerTimeout time.Duration
}

type MemoryTransport struct {
	mu  sync.RWMutex
	log *slog.Logger

	closed bool

	// shard -> subID -> handler
	shardSubs map[uint32]map[string]handlerFn

	// replyTo -> chan response bytes
	inboxes map[string]chan []byte

	seq uint64

	// Bounded concurrency
	sem            chan struct{}
	wg             sync.WaitGroup
	handlerTimeout time.Duration
}

func NewInMemoryTransport(opts ...MemoryTransportOpts) *MemoryTransport {
	var opt MemoryTransportOpts
	if len(opts) > 0 {
		opt = opts[0]
	}

	maxHandlers := opt.MaxConcurrentHandlers
	if maxHandlers == 0 {
		maxHandlers = DefaultMaxConcurrentHandlers
	}

	timeout := opt.HandlerTimeout
	if timeout == 0 {
		timeout = DefaultHandlerTimeout
	}

	t := &MemoryTransport{
		log:            slog.New(slog.DiscardHandler),
		shardSubs:      make(map[uint32]map[string]handlerFn),
		inboxes:        make(map[string]chan []byte),
		handlerTimeout: timeout,
	}

	// maxHandlers < 0 means unlimited (no semaphore)
	if maxHandlers > 0 {
		t.sem = make(chan struct{}, maxHandlers)
	}

	return t
}

func (t *MemoryTransport) WithLog(log *slog.Logger) *MemoryTransport {
	t.log = log.With(slog.String("transport", "mem"))
	return t
}

func (t *MemoryTransport) doPublish(ctx context.Context, env Envelope) error {
	// Check TTL before processing
	if env.Expired() {
		return ErrEnvelopeExpired
	}

	t.mu.RLock()
	if t.closed {
		t.mu.RUnlock()
		return ErrTransportClosed
	}

	// Copy handlers to avoid holding lock while invoking user code.
	subs := t.shardSubs[uint32(env.Shard)]
	handlers := make([]handlerFn, 0, len(subs))
	for _, h := range subs {
		handlers = append(handlers, h)
	}
	t.mu.RUnlock()

	if len(handlers) == 0 {
		return ErrTransportNoShardSubscriber
	}

	// If nobody is subscribed, drop events; for requests, the caller will time out.
	for _, h := range handlers {
		t.scheduleHandler(ctx, h, env)
	}

	return nil
}

func (t *MemoryTransport) Request(ctx context.Context, env Envelope) ([]byte, error) {
	// Validate envelope
	if err := env.Validate(); err != nil {
		return nil, err
	}

	// Set CreatedAtMs if TTL is set but CreatedAtMs is not
	if env.TTLMs > 0 && env.CreatedAtMs == 0 {
		env.CreatedAtMs = time.Now().UnixMilli()
	}

	// Create a per-request inbox
	replyTo := t.newInboxID()
	replyCh, err := t.registerInbox(replyTo)
	if err != nil {
		return nil, err
	}
	defer t.unregisterInbox(replyTo)

	env.ReplyTo = replyTo

	// Publish request (async delivery)
	if err := t.doPublish(ctx, env); err != nil {
		return nil, err
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case b, ok := <-replyCh:
		if !ok {
			return nil, ErrTransportClosed
		}
		var rf responseFrame
		if err := json.Unmarshal(b, &rf); err != nil {
			return nil, fmt.Errorf("decode response: %w", err)
		}
		if rf.Err != "" {
			return nil, errors.New(rf.Err)
		}
		return rf.Data, nil
	}
}

func (t *MemoryTransport) SubscribeShard(
	ctx context.Context,
	shardID uint32,
	h func(context.Context, Envelope) ([]byte, error),
) (Subscription, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.log.Debug("subscribe", slog.Int("shard", int(shardID)))

	if t.closed {
		return nil, ErrTransportClosed
	}
	if t.shardSubs[shardID] == nil {
		t.shardSubs[shardID] = make(map[string]handlerFn)
	}

	subID := t.newSubID(shardID)
	t.shardSubs[shardID][subID] = h

	s := &subscription{
		t:       t,
		log:     t.log.With(slog.String("subscription", subID), slog.String("shard", fmt.Sprintf("%d", shardID))),
		shardID: shardID,
		subID:   subID,
	}

	context.AfterFunc(ctx, func() {
		_ = s.Unsubscribe()
	})

	return s, nil
}

func (t *MemoryTransport) Close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true

	// Close all inbox channels so waiters unblock.
	for k, ch := range t.inboxes {
		close(ch)
		delete(t.inboxes, k)
	}

	// Clear subs
	for shard := range t.shardSubs {
		delete(t.shardSubs, shard)
	}
	t.mu.Unlock()

	// Wait for in-flight handlers to complete
	t.wg.Wait()

	t.log.Debug("closed")

	return nil
}

/* ---------------------- internals ---------------------- */

type subscription struct {
	t       *MemoryTransport
	log     *slog.Logger
	shardID uint32
	subID   string
	once    sync.Once
}

func (s *subscription) Unsubscribe() error {
	s.once.Do(func() {
		s.t.mu.Lock()
		defer s.t.mu.Unlock()
		if subs := s.t.shardSubs[s.shardID]; subs != nil {
			delete(subs, s.subID)
			if len(subs) == 0 {
				delete(s.t.shardSubs, s.shardID)
			}
		}
		s.log.Debug("unsubscribed")
	})
	return nil
}

func (t *MemoryTransport) scheduleHandler(ctx context.Context, h handlerFn, env Envelope) {
	t.wg.Add(1)
	go func() {
		defer t.wg.Done()

		// Acquire semaphore if bounded concurrency is enabled
		if t.sem != nil {
			select {
			case t.sem <- struct{}{}:
				defer func() { <-t.sem }()
			case <-ctx.Done():
				return
			}
		}

		t.invokeHandler(ctx, h, env)
	}()
}

func (t *MemoryTransport) invokeHandler(ctx context.Context, h handlerFn, env Envelope) {
	// Determine timeout: use handler timeout, but cap at remaining TTL if set
	timeout := t.handlerTimeout
	if env.TTLMs > 0 && env.CreatedAtMs > 0 {
		remaining := time.Duration(env.CreatedAtMs+env.TTLMs-time.Now().UnixMilli()) * time.Millisecond
		if remaining <= 0 {
			// Already expired, send timeout error if this is a request
			t.sendResponse(env, nil, ErrEnvelopeExpired)
			return
		}
		if remaining < timeout {
			timeout = remaining
		}
	}

	handlerCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	type result struct {
		data []byte
		err  error
	}
	done := make(chan result, 1)

	go func() {
		data, err := h(handlerCtx, env)
		done <- result{data, err}
	}()

	var resp []byte
	var err error

	select {
	case r := <-done:
		resp, err = r.data, r.err
	case <-handlerCtx.Done():
		if errors.Is(handlerCtx.Err(), context.DeadlineExceeded) {
			err = ErrHandlerTimeout
		} else {
			err = handlerCtx.Err()
		}
	}

	t.sendResponse(env, resp, err)
}

func (t *MemoryTransport) sendResponse(env Envelope, resp []byte, err error) {
	// If it's not a request, nothing to do.
	if env.ReplyTo == "" {
		if err != nil {
			t.log.Error("non-reply handler failed", slog.Any("envelope", env), slog.Any("error", err))
		}
		return
	}

	// Encode response (data + error)
	rf := responseFrame{Data: resp}
	if err != nil {
		rf.Err = err.Error()
		rf.Data = nil
	}
	b, _ := json.Marshal(rf)

	// Deliver response if inbox still exists
	t.mu.RLock()
	ch := t.inboxes[env.ReplyTo]
	t.mu.RUnlock()
	if ch == nil {
		t.log.Warn("dropping response", slog.String("replyTo", env.ReplyTo))
		return // requester timed out/canceled; drop
	}

	// Non-blocking send: if requester is gone or buffer full, drop.
	select {
	case ch <- b:
	default:
	}
}

func (t *MemoryTransport) newInboxID() string {
	n := atomic.AddUint64(&t.seq, 1)
	return fmt.Sprintf("inbox.%d", n)
}

func (t *MemoryTransport) newSubID(shardID uint32) string {
	n := atomic.AddUint64(&t.seq, 1)
	return fmt.Sprintf("sub.%d.%d", shardID, n)
}

func (t *MemoryTransport) registerInbox(replyTo string) (<-chan []byte, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return nil, ErrTransportClosed
	}
	// Buffered 1 so handler can respond even if requester is just about to select().
	ch := make(chan []byte, 1)
	t.inboxes[replyTo] = ch
	return ch, nil
}

func (t *MemoryTransport) unregisterInbox(replyTo string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	ch := t.inboxes[replyTo]
	if ch != nil {
		close(ch)
		delete(t.inboxes, replyTo)
	}
}
