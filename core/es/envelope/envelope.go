package envelope

import (
	"encoding/json"
	"fmt"
	"time"
)

// Envelope is what gets persisted in the event store.
type Envelope struct {
	ID            string          `json:"id"`           // ID is the message ID
	Seq           uint64          `json:"seq"`          // Seq is the global sequence number from the store
	Version       int             `json:"version"`      // 1..N, per aggregate stream
	AggregateType string          `json:"aggregate"`    // AggregateType is the aggregate root type
	AggregateID   string          `json:"aggregate_id"` // AggregateID is the aggregate root ID
	Type          string          `json:"type"`         // Type is the type of the event
	OccurredAt    time.Time       `json:"occurred_at"`
	Data          json.RawMessage `json:"data"`
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
