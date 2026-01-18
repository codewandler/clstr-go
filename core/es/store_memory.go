package es

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
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

	s.log.Debug("subscribe", slog.Any("options", options))

	// create subscription
	subID := gonanoid.Must()
	sub := &inMemorySubscription{
		opts: *options,
		ch:   make(chan Envelope, 1),
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

	//
	if options.deliverPolicy == DeliverAllPolicy {
		allEvents := make([]Envelope, 0)
		for _, events := range s.streams {
			allEvents = append(allEvents, events...)
		}
		if len(allEvents) == 0 {
			return sub, nil
		}
		sort.Slice(allEvents, func(i, j int) bool {
			return allEvents[i].Seq < allEvents[j].Seq
		})
		sub.maxSeq = allEvents[len(allEvents)-1].Seq
		go sub.dispatch(allEvents)

	}

	return sub, nil
}

func (s *InMemoryStore) dispatch(events []Envelope) {
	if len(s.subs) == 0 {
		return
	}

	var wg sync.WaitGroup
	wg.Add(len(s.subs))

	for _, sub := range s.subs {
		go func(events []Envelope) {
			defer wg.Done()
			sub.dispatch(events)
		}(events)
	}

	wg.Wait()
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
	expectVersion Version,
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
		curVersion = Version(0)
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
	mu     sync.Mutex
	opts   SubscribeOpts
	ch     chan Envelope
	cancel context.CancelFunc
	maxSeq uint64
}

func (i *inMemorySubscription) MaxSequence() uint64 {
	return i.maxSeq
}

func (i *inMemorySubscription) Chan() <-chan Envelope { return i.ch }
func (i *inMemorySubscription) Cancel()               { i.cancel() }

func (i *inMemorySubscription) dispatch(envelopes []Envelope) {
	i.mu.Lock()
	defer i.mu.Unlock()

	for _, e := range envelopes {
		if e.Seq < i.opts.startSequence {
			continue
		}

		if e.Version < i.opts.startVersion {
			continue
		}

		if !matchFilters(e, i.opts.filters) {
			continue
		}

		i.ch <- e
	}
}

var _ Subscription = (*inMemorySubscription)(nil)

var _ EventStore = (*InMemoryStore)(nil)
