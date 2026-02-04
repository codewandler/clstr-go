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
		Load(ctx context.Context, aggType string, aggID string, opts ...StoreLoadOption) ([]Envelope, error)

		// Append persists events to an aggregate stream with optimistic concurrency.
		// Returns ErrConcurrencyConflict if expectedVersion doesn't match the current version.
		Append(ctx context.Context, aggType string, aggID string, expectedVersion Version, events []Envelope) (*StoreAppendResult, error)
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
	if events == nil || len(events) == 0 {
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
