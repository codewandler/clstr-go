package es

import (
	"context"
	"errors"
)

var (
	ErrStoreNoEvents = errors.New("no events to store")
)

type (
	startVersionOption valueOption[int]
	startSeqOption     valueOption[uint64]

	eventStoreLoadOptions struct {
		startVersion int
		startSeq     uint64
	}

	storeLoadOptionsReceiver interface {
		SetStartVersion(int)
		SetStartSeq(uint64)
	}

	StoreLoadOption interface {
		ApplyToStoreLoadOptions(storeLoadOptionsReceiver)
	}
)

func (e *eventStoreLoadOptions) SetStartVersion(v int)    { e.startVersion = v }
func (e *eventStoreLoadOptions) SetStartSeq(seq uint64)   { e.startSeq = seq }
func WithStartAtVersion(startVersion int) StoreLoadOption { return startVersionOption{startVersion} }
func WithStartAtSeq(startSeq uint64) StoreLoadOption      { return startSeqOption{startSeq} }
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
		Load(ctx context.Context, aggType string, aggID string, opts ...StoreLoadOption) ([]Envelope, error)
		Append(ctx context.Context, aggType string, aggID string, expectedVersion int, events []Envelope) (*StoreAppendResult, error)
	}
)
