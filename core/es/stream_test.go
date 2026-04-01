package es_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/codewandler/clstr-go/core/es"
)

// TestMatchFilter_UsesAggregateType is a regression test for the bug where
// matchFilter compared env.Type (the event type, e.g. "UserCreatedEvent")
// against filter.AggregateType instead of env.AggregateType.
//
// The bug meant that filtering on AggregateType="User" would silently pass
// ALL events (because no event type string matches the aggregate type string),
// or reject events that happened to have the same type as an aggregate name.
func TestMatchFilter_UsesAggregateType(t *testing.T) {
	store := es.NewInMemoryStore()
	ctx := context.Background()

	// Subscribe filtered to "User" aggregate type only.
	sub, err := store.Subscribe(ctx,
		es.WithDeliverPolicy(es.DeliverAllPolicy),
		es.WithFilters(es.SubscribeFilter{AggregateType: "User"}),
	)
	require.NoError(t, err)
	t.Cleanup(sub.Cancel)

	// Append a User event — must arrive at the subscriber.
	_, err = store.Append(ctx, "User", "u-1", es.Version(0), []es.Envelope{{
		ID:            "ev-user-1",
		Type:          "UserCreatedEvent",
		AggregateType: "User",
		AggregateID:   "u-1",
		Version:       1,
		OccurredAt:    time.Now(),
	}})
	require.NoError(t, err)

	// Append an Order event — must be filtered out (AggregateType = "Order").
	_, err = store.Append(ctx, "Order", "o-1", es.Version(0), []es.Envelope{{
		ID:            "ev-order-1",
		Type:          "OrderPlacedEvent",
		AggregateType: "Order",
		AggregateID:   "o-1",
		Version:       1,
		OccurredAt:    time.Now(),
	}})
	require.NoError(t, err)

	// Should receive exactly the User event.
	select {
	case ev, ok := <-sub.Chan():
		require.True(t, ok)
		assert.Equal(t, "User", ev.AggregateType)
		assert.Equal(t, "UserCreatedEvent", ev.Type)
		assert.Equal(t, "u-1", ev.AggregateID)
	case <-time.After(time.Second):
		t.Fatal("no event received within 1s")
	}

	// Must NOT receive the Order event.
	select {
	case ev, ok := <-sub.Chan():
		if ok {
			t.Fatalf("unexpected event received: aggregateType=%s type=%s",
				ev.AggregateType, ev.Type)
		}
	case <-time.After(100 * time.Millisecond):
		// Correct: no spurious events.
	}
}

// TestMatchFilter_UsesAggregateID verifies that AggregateID filtering works
// and that events for a different aggregate ID are correctly excluded.
func TestMatchFilter_UsesAggregateID(t *testing.T) {
	store := es.NewInMemoryStore()
	ctx := context.Background()

	// Subscribe filtered to a specific User aggregate.
	sub, err := store.Subscribe(ctx,
		es.WithDeliverPolicy(es.DeliverAllPolicy),
		es.WithFilters(es.SubscribeFilter{AggregateType: "User", AggregateID: "u-target"}),
	)
	require.NoError(t, err)
	t.Cleanup(sub.Cancel)

	// Append an event for the target aggregate — must arrive.
	_, err = store.Append(ctx, "User", "u-target", es.Version(0), []es.Envelope{{
		ID:            "ev-target",
		Type:          "UserUpdatedEvent",
		AggregateType: "User",
		AggregateID:   "u-target",
		Version:       1,
		OccurredAt:    time.Now(),
	}})
	require.NoError(t, err)

	// Append an event for a different aggregate — must be filtered out.
	_, err = store.Append(ctx, "User", "u-other", es.Version(0), []es.Envelope{{
		ID:            "ev-other",
		Type:          "UserUpdatedEvent",
		AggregateType: "User",
		AggregateID:   "u-other",
		Version:       1,
		OccurredAt:    time.Now(),
	}})
	require.NoError(t, err)

	// Should receive the target event.
	select {
	case ev, ok := <-sub.Chan():
		require.True(t, ok)
		assert.Equal(t, "u-target", ev.AggregateID)
	case <-time.After(time.Second):
		t.Fatal("no event received within 1s")
	}

	// Must NOT receive the "u-other" event.
	select {
	case ev, ok := <-sub.Chan():
		if ok {
			t.Fatalf("unexpected event for aggregateID=%s", ev.AggregateID)
		}
	case <-time.After(100 * time.Millisecond):
		// Correct.
	}
}
