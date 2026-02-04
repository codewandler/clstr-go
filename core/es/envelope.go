package es

import (
	"encoding/json"
	"fmt"
	"time"
)

// Envelope wraps an event with metadata for persistence and routing.
// It is the unit of storage in the EventStore and contains all information
// needed to reconstruct and route events during replay or consumption.
type Envelope struct {
	// ID is the unique identifier of this event envelope.
	ID string `json:"id"`
	// Seq is the global sequence number assigned by the store.
	// This provides total ordering across all events in the store.
	Seq uint64 `json:"seq"`
	// Version is the per-aggregate stream version (1, 2, 3, ...).
	// Used for optimistic concurrency control.
	Version Version `json:"version"`
	// AggregateType identifies the type of aggregate this event belongs to.
	AggregateType string `json:"aggregate"`
	// AggregateID identifies the specific aggregate instance.
	AggregateID string `json:"aggregate_id"`
	// Type is the event type name for deserialization routing.
	Type string `json:"type"`
	// OccurredAt is when the event was created.
	OccurredAt time.Time `json:"occurred_at"`
	// Data contains the JSON-encoded event payload.
	Data json.RawMessage `json:"data"`
}

func (e Envelope) Validate() error {
	if e.ID == "" {
		return fmt.Errorf("envelope id is empty")
	}
	if e.OccurredAt.IsZero() {
		return fmt.Errorf("envelope occurred at is zero")
	}
	if e.AggregateID == "" {
		return fmt.Errorf("envelope aggregate id is empty")
	}
	if e.AggregateType == "" {
		return fmt.Errorf("envelope aggregate type is empty")
	}
	if e.Type == "" {
		return fmt.Errorf("envelope type is empty")
	}
	return nil
}

type Decoder interface{ Decode(e Envelope) (any, error) }
