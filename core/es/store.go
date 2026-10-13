package es

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	gonanoid "github.com/matoous/go-nanoid/v2"

	"github.com/codewandler/clstr-go/core/reflector"
)

var (
	ErrStoreNoEvents = errors.New("no events to store")
)

type (
	startVersionOption valueOption[Version]
	StartSeqOption     valueOption[uint64]

	eventStoreLoadOptions struct {
		startVersion Version
		startSeq     uint64
	}

	storeLoadOptionsReceiver interface {
		SetStartVersion(Version)
		SetStartSeq(uint64)
	}

	StoreLoadOption interface {
		ApplyToStoreLoadOptions(storeLoadOptionsReceiver)
	}
)

func (e *eventStoreLoadOptions) SetStartVersion(v Version) { e.startVersion = v }
func (e *eventStoreLoadOptions) SetStartSeq(seq uint64)    { e.startSeq = seq }
func WithStartAtVersion(startVersion Version) StoreLoadOption {
	return startVersionOption{startVersion}
}
func WithStartSeq(startSeq uint64) StartSeqOption { return StartSeqOption{startSeq} }
func (o startVersionOption) ApplyToStoreLoadOptions(receiver storeLoadOptionsReceiver) {
	receiver.SetStartVersion(o.v)
}
func (o StartSeqOption) ApplyToStoreLoadOptions(receiver storeLoadOptionsReceiver) {
	receiver.SetStartSeq(o.v)
}

type (
	// StoreAppendOptions holds the optional arguments of EventStore.Append.
	StoreAppendOptions struct {
		// LastKnownSeq is the global stream sequence the caller's state of the
		// aggregate reflects: the sequence of its last loaded event, or of the
		// snapshot it was restored from when no later event was loaded.
		// Zero means unknown.
		LastKnownSeq uint64
	}

	// StoreAppendOption configures a single EventStore.Append call.
	StoreAppendOption func(*StoreAppendOptions)
)

// WithLastKnownSeq tells Append which stream sequence the caller's state of the
// aggregate reflects. A store whose stream trims old events uses it to accept
// the first append after every stored event of the aggregate was trimmed; see
// EventStore.Append.
func WithLastKnownSeq(seq uint64) StoreAppendOption {
	return func(o *StoreAppendOptions) { o.LastKnownSeq = seq }
}

// NewStoreAppendOptions applies opts to a zero StoreAppendOptions.
func NewStoreAppendOptions(opts ...StoreAppendOption) StoreAppendOptions {
	var o StoreAppendOptions
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	return o
}

type (
	// StoreAppendResult contains the result of appending events to the store.
	StoreAppendResult struct {
		// LastSeq is the global sequence number assigned to the last appended event.
		LastSeq uint64
	}

	// EventStore is the persistence interface for event sourcing.
	// It provides operations for loading and appending events to aggregate streams,
	// as well as subscribing to the global event stream for consumers.
	//
	// Implementations must guarantee:
	//   - Atomic append of multiple events within a single Append call
	//   - Optimistic concurrency via expectedVersion check
	//   - Ordered delivery of events by sequence number in subscriptions
	EventStore interface {
		// Stream provides subscription capabilities for consumers.
		Stream

		// Load retrieves events for a specific aggregate stream.
		// Events are returned in version order. Use opts to filter by version/sequence.
		//
		// When WithStartAtVersion is given and the first stored event at or
		// after the start sequence has a higher version because the stream's
		// retention limits trimmed the events in between, a store that can
		// tell returns an error wrapping ErrHistoryTrimmed.
		Load(ctx context.Context, aggType string, aggID string, opts ...StoreLoadOption) ([]Envelope, error)

		// Append persists events to an aggregate stream with optimistic concurrency.
		// Returns ErrConcurrencyConflict if expectedVersion doesn't match the current version.
		//
		// One exception, for stores whose stream trims old events: when the
		// aggregate has no stored events left, expectedVersion > 0, and
		// WithLastKnownSeq names a sequence below the stream's first sequence
		// (the caller's state was restored from a snapshot whose events were
		// all trimmed), the store accepts the events as the continuation of
		// expectedVersion, provided the aggregate still has no stored events
		// at the moment of the write.
		Append(ctx context.Context, aggType string, aggID string, expectedVersion Version, events []Envelope, opts ...StoreAppendOption) (*StoreAppendResult, error)
	}
)

func AppendEvents(
	ctx context.Context,
	store EventStore,
	aggType string,
	aggID string,
	expect Version,
	events ...any,
) (*StoreAppendResult, error) {
	if len(events) == 0 {
		return nil, ErrStoreNoEvents
	}
	envelopes := make([]Envelope, 0)
	for i, ev := range events {
		data, err := json.Marshal(ev)
		if err != nil {
			return nil, err
		}
		envelopes = append(envelopes, Envelope{
			ID:            gonanoid.Must(),
			Type:          reflector.TypeInfoOf(ev).Name,
			AggregateID:   aggID,
			AggregateType: aggType,
			Data:          data,
			OccurredAt:    time.Now(),
			Version:       expect + Version(i+1),
		})
	}
	return store.Append(
		ctx,
		aggType,
		aggID,
		expect,
		envelopes,
	)
}
