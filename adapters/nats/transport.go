package nats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"

	natsgo "github.com/nats-io/nats.go"

	"github.com/codewandler/clstr-go/core/cluster"
)

type TransportConfig struct {
	Connect       Connector    // Connect is used to create the underlying NATS connection. If nil, ConnectDefault() is used.
	Log           *slog.Logger // Log for diagnostics (optional)
	SubjectPrefix string       // SubjectPrefix for shard subjects, e.g. "clstr" -> clstr.shard.<id>
}

type Transport struct {
	nc      *natsgo.Conn
	closeNc closeFunc
	log     *slog.Logger
	prefix  string

	mu   sync.Mutex
	subs map[*natsgo.Subscription]struct{}

	closed atomic.Bool
}

// responseFrame is the minimal response encoding for Request(). Must match core/cluster in-memory transport.
type responseFrame struct {
	Data []byte `json:"data,omitempty"`
	Err  string `json:"err,omitempty"`
}

func NewTransport(cfg TransportConfig) (*Transport, error) {
	connFn := cfg.Connect
	if connFn == nil {
		connFn = ConnectDefault()
	}

	log := cfg.Log
	if log == nil {
		log = slog.Default()
	}

	nc, closeNc, err := connFn()
	if err != nil {
		return nil, err
	}

	t := &Transport{
		nc:      nc,
		closeNc: closeNc,
		log:     log.With(slog.String("transport", "nats")),
		prefix:  cfg.SubjectPrefix,
		subs:    make(map[*natsgo.Subscription]struct{}),
	}

	return t, nil
}

// subjectShard returns the subject used for a shard.
func (t *Transport) subjectShard(shardID uint32) string {
	p := t.prefix
	if p == "" {
		p = "clstr"
	}
	return p + ".shard." + strconv.FormatUint(uint64(shardID), 10)
}

func (t *Transport) Request(ctx context.Context, env cluster.Envelope) ([]byte, error) {
	if t.closed.Load() {
		return nil, cluster.ErrTransportClosed
	}

	// Create a reply inbox and subscription
	inbox := natsgo.NewInbox()
	ch := make(chan *natsgo.Msg, 1)
	sub, err := t.nc.ChanSubscribe(inbox, ch)
	if err != nil {
		return nil, fmt.Errorf("nats: subscribe inbox: %w", err)
	}
	defer func() {
		_ = sub.Unsubscribe()
		close(ch)
	}()

	env.ReplyTo = inbox

	// Encode envelope
	payload, err := json.Marshal(env)
	if err != nil {
		return nil, fmt.Errorf("encode envelope: %w", err)
	}

	// Publish to shard subject
	subj := t.subjectShard(uint32(env.Shard))
	if err := t.nc.Publish(subj, payload); err != nil {
		return nil, fmt.Errorf("nats: publish: %w", err)
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case msg, ok := <-ch:
		if !ok {
			return nil, cluster.ErrTransportClosed
		}
		var rf responseFrame
		if err := json.Unmarshal(msg.Data, &rf); err != nil {
			return nil, fmt.Errorf("decode response: %w", err)
		}
		if rf.Err != "" {
			return nil, errors.New(rf.Err)
		}
		return rf.Data, nil
	}
}

func (t *Transport) Close() error {
	if t.closed.Swap(true) {
		return cluster.ErrTransportClosed
	}
	t.mu.Lock()
	for s := range t.subs {
		_ = s.Unsubscribe()
	}
	t.subs = map[*natsgo.Subscription]struct{}{}
	t.mu.Unlock()
	if t.nc != nil {
		t.nc.Drain()
		t.closeNc()
	}
	return nil
}

// Close allows tests to call Close with a context, but it simply proxies to Close().
func (t *Transport) CloseContext(_ context.Context) error { // helper, not part of interface
	return t.Close()
}

// SubscribeShard subscribes to messages for a specific shard.
func (t *Transport) SubscribeShard(ctx context.Context, shardID uint32, h cluster.ServerHandlerFunc) (cluster.Subscription, error) {
	if t.closed.Load() {
		return nil, cluster.ErrTransportClosed
	}
	subj := t.subjectShard(shardID)

	sub, err := t.nc.Subscribe(subj, func(msg *natsgo.Msg) {
		// Decode envelope
		var env cluster.Envelope
		if err := json.Unmarshal(msg.Data, &env); err != nil {
			t.log.Error("failed to decode envelope", slog.Any("error", err))
			return
		}

		// Invoke handler
		data, err := h(ctx, env)
		rf := responseFrame{Data: data}
		if err != nil {
			rf.Err = err.Error()
		}
		b, _ := json.Marshal(rf)

		// Publish reply if requested
		if env.ReplyTo != "" {
			if err := t.nc.Publish(env.ReplyTo, b); err != nil {
				t.log.Error("failed to publish reply", slog.Any("error", err))
			}
		}
	})
	if err != nil {
		return nil, fmt.Errorf("nats: subscribe shard: %w", err)
	}

	t.mu.Lock()
	t.subs[sub] = struct{}{}
	t.mu.Unlock()

	// Handle context cancellation by auto-unsubscribing
	go func() {
		<-ctx.Done()
		_ = sub.Unsubscribe()
		t.mu.Lock()
		delete(t.subs, sub)
		t.mu.Unlock()
	}()

	return &subscription{sub: sub, t: t}, nil
}

type subscription struct {
	sub *natsgo.Subscription
	t   *Transport
}

func (s *subscription) Unsubscribe() error {
	if s.sub == nil {
		return nil
	}
	err := s.sub.Unsubscribe()
	s.t.mu.Lock()
	delete(s.t.subs, s.sub)
	s.t.mu.Unlock()
	return err
}

var _ cluster.Transport = &Transport{}
