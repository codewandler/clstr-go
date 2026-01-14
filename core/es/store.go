package es

import (
	"context"
	"errors"

	"github.com/codewandler/clstr-go/core/es/envelope"
	"github.com/codewandler/clstr-go/core/es/types"
)

var (
	ErrStoreNoEvents = errors.New("no events to store")
)

type (
	startVersionOption valueOption[types.Version]
	startSeqOption     valueOption[uint64]

	eventStoreLoadOptions struct {
		startVersion types.Version
		startSeq     uint64
	}

	storeLoadOptionsReceiver interface {
		SetStartVersion(types.Version)
		SetStartSeq(uint64)
	}

	StoreLoadOption interface {
		ApplyToStoreLoadOptions(storeLoadOptionsReceiver)
	}
)

func (e *eventStoreLoadOptions) SetStartVersion(v types.Version) { e.startVersion = v }
func (e *eventStoreLoadOptions) SetStartSeq(seq uint64)          { e.startSeq = seq }
func WithStartAtVersion(startVersion types.Version) StoreLoadOption {
	return startVersionOption{startVersion}
}
func WithStartAtSeq(startSeq uint64) StoreLoadOption { return startSeqOption{startSeq} }
func (o startVersionOption) ApplyToStoreLoadOptions(receiver storeLoadOptionsReceiver) {
	receiver.SetStartVersion(o.v)
}
func (o startSeqOption) ApplyToStoreLoadOptions(receiver storeLoadOptionsReceiver) {
	receiver.SetStartSeq(o.v)
}

// EventStore stores and loads envelopes per aggregate stream.
type (
	StoreAppendResult struct {
		LastSeq uint64
	}

	EventStore interface {
		Stream
		Load(ctx context.Context, aggType string, aggID string, opts ...StoreLoadOption) ([]envelope.Envelope, error)
		Append(ctx context.Context, aggType string, aggID string, expectedVersion types.Version, events []envelope.Envelope) (*StoreAppendResult, error)
	}
)
