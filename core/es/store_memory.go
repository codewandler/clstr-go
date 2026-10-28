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

	// firstSeq is the lowest sequence TrimBefore kept; 0 until it is called.
	firstSeq uint64
}

// TrimBefore removes every event whose sequence is below seq, the way a
// stream's retention limits (age, size or count) drop its oldest messages.
// It lets tests reproduce an aggregate whose snapshot outlived its events:
// afterwards Load and Append behave as the NATS store does on a trimmed
// stream. Events appended later get sequences at or above seq.
func (s *InMemoryStore) TrimBefore(seq uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if seq <= s.firstSeq {
		return
	}
	s.firstSeq = seq
	for k, events := range s.streams {
		kept := make([]Envelope, 0, len(events))
		for _, e := range events {
			if e.Seq >= seq {
				kept = append(kept, e)
			}
		}
		s.streams[k] = kept
	}
	if s.seq.Load() < seq-1 {
		s.seq.Store(seq - 1)
	}
}

// trimmedPast reports whether TrimBefore removed the event at seq. Zero means
// unknown and is never reported as trimmed. Callers hold s.mu.
func (s *InMemoryStore) trimmedPast(seq uint64) bool {
	return seq > 0 && s.firstSeq > seq
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
		done: make(chan error), // never written to; in-memory subscriptions don't fail
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

	// The first event skips past the requested start version and the
	// skipped events were trimmed: report it as the NATS store does.
	if loadOpts.startVersion > 0 && len(out) > 0 && out[0].Version > loadOpts.startVersion {
		from := max(loadOpts.startSeq, 1)
		if s.trimmedPast(from) {
			return nil, &HistoryTrimmedError{
				AggType:            aggType,
				AggID:              aggID,
				StartVersion:       loadOpts.startVersion,
				StartSeq:           from,
				FirstStoredVersion: out[0].Version,
				FirstStoredSeq:     out[0].Seq,
				StreamFirstSeq:     s.firstSeq,
			}
		}
	}

	return out, nil
}

func (s *InMemoryStore) Append(
	_ context.Context,
	aggType string,
	aggID string,
	expectVersion Version,
	events []Envelope,
	opts ...StoreAppendOption,
) (*StoreAppendResult, error) {
	if len(events) == 0 {
		return nil, ErrStoreNoEvents
	}
	appendOpts := NewStoreAppendOptions(opts...)

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
	// An aggregate with no stored events continues at expectVersion only when
	// TrimBefore removed the event the caller's state was built from; see
	// EventStore.Append. The lock makes the check and the write atomic.
	trimmedContinuation := len(curStream) == 0 && expectVersion > 0 && s.trimmedPast(appendOpts.LastKnownSeq)
	if curVersion != expectVersion && !trimmedContinuation {
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
	done   chan error
	cancel context.CancelFunc
	maxSeq uint64
}

func (i *inMemorySubscription) MaxSequence() uint64   { return i.maxSeq }
func (i *inMemorySubscription) Chan() <-chan Envelope { return i.ch }
func (i *inMemorySubscription) Cancel()               { i.cancel() }
func (i *inMemorySubscription) Done() <-chan error    { return i.done }

func (i *inMemorySubscription) dispatch(envelopes []Envelope) {
	// Filter under lock to protect opts from concurrent mutation.
	i.mu.Lock()
	filtered := make([]Envelope, 0, len(envelopes))
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
		filtered = append(filtered, e)
	}
	i.mu.Unlock()

	// Send without holding the lock so slow consumers don't block
	// concurrent dispatches or other subscription operations.
	for _, e := range filtered {
		i.ch <- e
	}
}

var _ Subscription = (*inMemorySubscription)(nil)

var _ EventStore = (*InMemoryStore)(nil)
