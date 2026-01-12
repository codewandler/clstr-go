package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
)

// responseFrame is the minimal response encoding for Request().
// Transport remains backend-agnostic because it's just bytes on the wire.
type responseFrame struct {
	Data []byte `json:"data,omitempty"`
	Err  string `json:"err,omitempty"`
}

type handlerFn func(context.Context, Envelope) ([]byte, error)

type MemoryTransport struct {
	mu  sync.RWMutex
	log *slog.Logger

	closed bool

	// shard -> subID -> handler
	shardSubs map[uint32]map[string]handlerFn

	// replyTo -> chan response bytes
	inboxes map[string]chan []byte

	seq uint64
}

func NewInMemoryTransport() *MemoryTransport {
	return &MemoryTransport{
		log:       slog.New(slog.DiscardHandler),
		shardSubs: make(map[uint32]map[string]handlerFn),
		inboxes:   make(map[string]chan []byte),
	}
}

func (t *MemoryTransport) WithLog(log *slog.Logger) *MemoryTransport {
	t.log = log.With(slog.String("transport", "mem"))
	return t
}

func (t *MemoryTransport) doPublish(ctx context.Context, env Envelope) error {

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
		h := h
		go t.invokeHandler(ctx, h, env)
	}

	return nil
}

func (t *MemoryTransport) Request(ctx context.Context, env Envelope) ([]byte, error) {
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
	defer t.mu.Unlock()

	if t.closed {
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

func (t *MemoryTransport) invokeHandler(ctx context.Context, h handlerFn, env Envelope) {
	resp, err := h(ctx, env)

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
