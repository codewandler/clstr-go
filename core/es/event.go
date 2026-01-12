package es

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/codewandler/clstr-go/internal/reflector"
)

// Envelope is what gets persisted in the event store.
type Envelope struct {
	ID          string          `json:"id"`           // ID is the message ID
	Seq         uint64          `json:"seq"`          // Seq is the global sequence number from the store
	Version     int             `json:"version"`      // 1..N, per aggregate stream
	AggregateID string          `json:"aggregate_id"` // AggregateID is the aggregate root ID
	Type        string          `json:"type"`         // Type is the type of the event
	OccurredAt  time.Time       `json:"occurred_at"`
	Data        json.RawMessage `json:"data"`
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
	if e.Type == "" {
		return fmt.Errorf("envelope type is empty")
	}
	return nil
}

// EventRegistry maps event type names to constructors so we can decode persisted events.
type EventRegistry struct {
	mu   sync.RWMutex
	news map[string]func() any
}

func NewRegistry() *EventRegistry {
	return &EventRegistry{news: map[string]func() any{}}
}

func (r *EventRegistry) Register(eventType string, ctor func() any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.news[eventType] = ctor
}

func (r *EventRegistry) Decode(env Envelope) (any, error) {
	r.mu.RLock()
	ctor, ok := r.news[env.Type]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrUnknownEventType, env.Type)
	}
	ev := ctor()
	if env.Data != nil {
		if err := json.Unmarshal(env.Data, ev); err != nil {
			return nil, err
		}
	}
	return ev, nil
}

type Registrar interface {
	Register(eventType string, ctor func() any)
}

func RegisterEventFor[T any](r Registrar) {
	ti := reflector.TypeInfoFor[T]()
	r.Register(ti.Name, func() any {
		return any(new(T))
	})
}

// Event returns a reflection-free constructor for an event of type T.
// Each call to the returned function constructs a fresh *T via new(T).
func Event[T any]() func() any { return func() any { return new(T) } }

// RegisterEvents registers event constructors. It does not use reflection to create instances.
// For each provided constructor, we call it once to determine the event type name and then
// register the original constructor so future decodes produce fresh instances per call.
func RegisterEvents(r Registrar, ctors ...func() any) {
	for _, ctor := range ctors {
		// Create a temporary instance to derive the type name and metadata
		sample := ctor()
		eventType := getEventTypeOf(sample)
		r.Register(eventType, ctor)
	}
}

func getEventTypeOf(ev any) (eventType string) {
	switch t := ev.(type) {
	case interface{ EventType() string }:
		eventType = t.EventType()
	// TODO: event meta
	default:
		eventType = reflector.TypeInfoOf(ev).Name
	}
	return
}
