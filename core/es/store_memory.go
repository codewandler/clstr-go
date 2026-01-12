package es

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	gonanoid "github.com/matoous/go-nanoid/v2"
)

// InMemoryStore is a simple, correct (optimistic) store for tests/dev.
type InMemoryStore struct {
	mu      sync.Mutex
	log     *slog.Logger
	seq     atomic.Uint64
	streams map[string][]Envelope
	subs    map[string]*inMemorySubscription
}

func (s *InMemoryStore) Subscribe(ctx context.Context, opts ...SubscribeOption) (Subscription, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	options := &SubscribeOpts{
		deliverPolicy: DeliverNewPolicy,
		filters:       []SubscribeFilter{},
	}

	for _, opt := range opts {
		opt(options)
	}

	// create subscription
	subID := gonanoid.Must()
	sub := &inMemorySubscription{
		filters: options.filters,
		ch:      make(chan Envelope, 1),
		cancel: func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			delete(s.subs, subID)
		},
	}
	s.subs[subID] = sub

	context.AfterFunc(ctx, func() {
		sub.Cancel()
	})

	fmt.Printf("SUBSCRIBE %+v\n", options)

	//
	if options.deliverPolicy == DeliverAllPolicy {
		for k, _ := range s.streams {
			go func(k string) {
				for _, e := range s.streams[k] {

					if e.Seq < options.startSequence {
						continue
					}
					if e.Version < options.startVersion {
						continue
					}

					if matchFilters(e, sub.filters) {
						sub.ch <- e
					}
				}
			}(k)
		}
	}

	return sub, nil
}

func (s *InMemoryStore) dispatch(events []Envelope) {
	if len(s.subs) == 0 {
		return
	}

	s.log.Debug(
		"dispatching events",
		slog.Int("events", len(events)),
		slog.Int("subscriptions", len(s.subs)),
	)

	for _, e := range events {
		for _, sub := range s.subs {
			if matchFilters(e, sub.filters) {
				sub.ch <- e
			}
		}
	}
}

func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		log:     slog.Default().With(slog.String("store", "memory")),
		streams: map[string][]Envelope{},
		subs:    map[string]*inMemorySubscription{},
	}
}

func (s *InMemoryStore) streamKey(aggType, aggID string) string {
	return fmt.Sprintf("%s-%s", aggType, aggID)
}

func (s *InMemoryStore) Load(
	_ context.Context,
	aggType,
	aggID string,
	opts ...StoreLoadOption,
) ([]Envelope, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// init load options
	loadOpts := &eventStoreLoadOptions{}
	for _, opt := range opts {
		opt.ApplyToStoreLoadOptions(loadOpts)
	}

	// get stream
	sk := s.streamKey(aggType, aggID)
	events, ok := s.streams[sk]
	if !ok {
		return nil, ErrAggregateNotFound
	}

	out := make([]Envelope, 0)
	for _, e := range events {
		if e.Version < loadOpts.startVersion {
			continue
		}
		if e.Seq < loadOpts.startSeq {
			continue
		}
		out = append(out, e)
	}

	return out, nil
}

func (s *InMemoryStore) Append(
	_ context.Context,
	aggType string,
	aggID string,
	expectVersion int,
	events []Envelope,
) (*StoreAppendResult, error) {
	if len(events) == 0 {
		return nil, ErrStoreNoEvents
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var (
		sk         = s.streamKey(aggType, aggID)
		curStream  = s.streams[sk]
		curVersion = 0
	)

	if len(curStream) > 0 {
		curVersion = curStream[len(curStream)-1].Version
	}
	if curVersion != expectVersion {
		return nil, ErrConcurrencyConflict
	}

	// add sequence number
	var (
		lastSeq   uint64
		allEvents = make([]Envelope, 0)
	)
	for _, e := range events {
		// validate event
		if v, ok := any(e).(interface{ Validate() error }); ok {
			if err := v.Validate(); err != nil {
				return nil, err
			}
		}

		lastSeq = s.seq.Add(1)
		e.Seq = lastSeq
		allEvents = append(allEvents, e)
	}
	s.streams[sk] = append(curStream, allEvents...)
	s.log.Debug(
		"append",
		slog.Uint64("last_seq", lastSeq),
		slog.Int("num_events", len(allEvents)),
	)

	s.dispatch(allEvents)

	return &StoreAppendResult{LastSeq: lastSeq}, nil
}

// === Subscription ===

type inMemorySubscription struct {
	filters []SubscribeFilter
	ch      chan Envelope
	cancel  context.CancelFunc
}

func (i *inMemorySubscription) Chan() <-chan Envelope { return i.ch }
func (i *inMemorySubscription) Cancel()               { i.cancel() }

var _ EventStore = (*InMemoryStore)(nil)
