package es

import (
	"context"
	"errors"
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

// EventStore stores and loads envelopes per aggregate stream.
type (
	StoreAppendResult struct {
		LastSeq uint64
	}

	EventStore interface {
		Stream
		Load(ctx context.Context, aggType string, aggID string, opts ...StoreLoadOption) ([]Envelope, error)
		Append(ctx context.Context, aggType string, aggID string, expectedVersion Version, events []Envelope) (*StoreAppendResult, error)
	}
)
