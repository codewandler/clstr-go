package es

import (
	"errors"
	"fmt"
	"time"

	"github.com/codewandler/clstr-go/core/es/assert"
)

var (
	ErrAggregateNotFound   = errors.New("aggregate not found")
	ErrConcurrencyConflict = errors.New("concurrency conflict")
	ErrUnknownEventType    = errors.New("unknown event type")
)

// events
type (
	// AggregateDeleted is an event that marks an aggregate as deleted.
	AggregateDeleted struct{}
)

// Applier is the interface for types that can apply events to update their state.
type Applier interface {
	Apply(event any) error
}

// Aggregate is the core interface for event-sourced domain objects.
// It defines the contract that all aggregate roots must implement to work
// with the Repository for loading and persisting state through events.
//
// An aggregate maintains:
//   - Identity: type and ID that uniquely identify the aggregate stream
//   - Version: the current version for optimistic concurrency control
//   - Sequence: the global stream sequence number of the last applied event
//   - Uncommitted events: events raised but not yet persisted
//
// The typical lifecycle is:
//  1. Create a new aggregate or load an existing one via Repository
//  2. Execute domain logic that calls Raise() to record events
//  3. Apply() is called to update internal state from each event
//  4. Save via Repository which persists uncommitted events and calls ClearUncommitted()
type Aggregate interface {
	// GetAggType returns the aggregate type name used for stream identification.
	GetAggType() string
	// GetID returns the unique identifier of this aggregate instance.
	GetID() string
	// SetID sets the aggregate ID. Typically called during creation.
	SetID(string)

	// GetVersion returns the current version (number of events applied).
	GetVersion() Version
	setVersion(Version)

	// GetSeq returns the global stream sequence of the last applied event.
	GetSeq() uint64
	setSeq(uint64)

	// Create initializes a new aggregate with the given ID.
	Create(id string) error

	// Register registers event types with the provided Registrar.
	Register(r Registrar)
	// Raise records an event as uncommitted without applying it.
	Raise(event any)
	// Apply updates the aggregate state from an event.
	Apply(event any) error

	// Uncommitted returns a copy of events raised but not yet persisted.
	Uncommitted() []any
	// ClearUncommitted removes all uncommitted events after successful save.
	ClearUncommitted()
}

type AggregateCreatedEvent struct {
	ID        string    `json:"id"`
	CreatedAt time.Time `json:"created_at"`
}

func (e AggregateCreatedEvent) Validate() error {

	if e.CreatedAt.IsZero() {
		return errors.New("created at time is zero")
	}

	if e.ID == "" {
		return errors.New("id is required")
	}

	return nil

}

// BaseAggregate is an embeddable helper that tracks version + uncommitted events.
type BaseAggregate struct {
	CreatedAt time.Time `json:"created_at"`

	id          string
	version     Version
	seq         uint64
	uncommitted []any
}

func (b *BaseAggregate) Apply(evt any) error {
	switch e := evt.(type) {
	case *AggregateCreatedEvent:
		b.CreatedAt = e.CreatedAt
		b.id = e.ID
		return nil
	}
	return fmt.Errorf("unknown base aggregate event: %T", evt)
}

func (b *BaseAggregate) IsCreated() bool         { return b.CreatedAt.IsZero() == false }
func (b *BaseAggregate) GetCreatedAt() time.Time { return b.CreatedAt }

func (b *BaseAggregate) Create(id string) error {
	if b.IsCreated() {
		return fmt.Errorf("aggregate already created")
	}
	if id == "" {
		return fmt.Errorf("id is required")
	}
	return RaiseAndApply(b, &AggregateCreatedEvent{ID: id, CreatedAt: time.Now()})
}

func (b *BaseAggregate) GetID() string        { return b.id }
func (b *BaseAggregate) SetID(id string)      { b.id = id }
func (b *BaseAggregate) GetVersion() Version  { return b.version }
func (b *BaseAggregate) setVersion(v Version) { b.version = v }
func (b *BaseAggregate) GetSeq() uint64       { return b.seq }
func (b *BaseAggregate) setSeq(s uint64)      { b.seq = s }

// Raise records an event as uncommitted.
// (Typically you call Raise+Apply together via a helper like ApplyNew below.)
func (b *BaseAggregate) Raise(event any)   { b.uncommitted = append(b.uncommitted, event) }
func (b *BaseAggregate) ClearUncommitted() { b.uncommitted = nil }
func (b *BaseAggregate) Uncommitted() []any {
	out := make([]any, len(b.uncommitted))
	copy(out, b.uncommitted)
	return out
}

func (b *BaseAggregate) Checked(c assert.Cond, thenFunc func() error) error {
	err := c.Check()
	if err != nil {
		return err
	}
	return thenFunc()
}

// === Helpers ===

type raiseApplier interface {
	Raise(event any)
	Apply(event any) error
}

// RaiseAndApply records e as uncommitted and applies it to mutate state.
func RaiseAndApply(a raiseApplier, events ...any) (err error) {
	if len(events) == 0 {
		return
	}

	// validate
	for _, e := range events {
		if ev, ok := e.(interface{ Validate() error }); ok {
			err = ev.Validate()
			if err != nil {
				return fmt.Errorf("invalid event %T: %w", ev, err)
			}
		}
	}

	for _, e := range events {
		a.Raise(e)
		err = a.Apply(e)
		if err != nil {
			return
		}
	}
	return
}

func RaiseAndApplyD(a Aggregate, events ...any) func() error {
	return func() error {
		return RaiseAndApply(a, events...)
	}
}
