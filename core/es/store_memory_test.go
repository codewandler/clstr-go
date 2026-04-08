package es_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/codewandler/clstr-go/core/es"
)

var testNow = time.Now()

// TestInMemoryStore_DispatchDoesNotDeadlock verifies that a slow consumer
// on one subscription does not block event delivery to other subscriptions.
//
// Before the fix, dispatch() held the subscription mutex for the entire send
// loop — a slow consumer would block the lock and prevent Append from
// returning, deadlocking all producers.
func TestInMemoryStore_DispatchDoesNotDeadlock(t *testing.T) {
	store := es.NewInMemoryStore()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// sub1: slow consumer — reads events with a deliberate delay.
	sub1, err := store.Subscribe(ctx,
		es.WithDeliverPolicy(es.DeliverAllPolicy),
		es.WithStartSequence(1),
	)
	require.NoError(t, err)

	// sub2: fast consumer — reads events immediately.
	sub2, err := store.Subscribe(ctx,
		es.WithDeliverPolicy(es.DeliverAllPolicy),
		es.WithStartSequence(1),
	)
	require.NoError(t, err)

	// Goroutine: slowly drain sub1 (simulates a consumer that processes slowly).
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 3 {
			select {
			case <-sub1.Chan():
				time.Sleep(200 * time.Millisecond) // simulate slow processing
			case <-time.After(3 * time.Second):
				return
			}
		}
	}()

	// Goroutine: quickly drain sub2.
	sub2Events := make([]es.Envelope, 0, 3)
	var mu sync.Mutex
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 3 {
			select {
			case ev := <-sub2.Chan():
				mu.Lock()
				sub2Events = append(sub2Events, ev)
				mu.Unlock()
			case <-time.After(3 * time.Second):
				return
			}
		}
	}()

	// Append 3 events. Before the fix, each Append would block waiting for
	// the slow consumer, making the total time ~600ms (3 × 200ms).
	// After the fix, Append returns immediately and the slow consumer
	// drains asynchronously.
	start := time.Now()
	for i := range 3 {
		_, err := store.Append(ctx, "test", "agg1", es.Version(i), []es.Envelope{{
			ID:            string(rune('a' + i)),
			Type:          "TestEvent",
			AggregateID:   "agg1",
			AggregateType: "test",
			Version:       es.Version(i + 1),
			OccurredAt:    testNow,
		}})
		require.NoError(t, err)
	}
	appendDuration := time.Since(start)

	// Appends should complete quickly — not blocked by the slow consumer.
	assert.Less(t, appendDuration, 500*time.Millisecond,
		"Append should not be blocked by a slow subscription consumer")

	// Wait for both consumers to finish processing.
	wg.Wait()

	// sub2 should have received all 3 events promptly.
	mu.Lock()
	assert.Len(t, sub2Events, 3, "fast consumer should receive all 3 events")
	mu.Unlock()
}

// TestInMemoryStore_DispatchFilterUnderLock verifies that dispatch still
// correctly filters events by sequence and subscription options.
func TestInMemoryStore_DispatchFilterUnderLock(t *testing.T) {
	store := es.NewInMemoryStore()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Append 2 events first.
	_, err := store.Append(ctx, "test", "agg1", 0, []es.Envelope{
		{ID: "e1", Type: "TestEvent", AggregateID: "agg1", AggregateType: "test", Version: 1, OccurredAt: testNow},
	})
	require.NoError(t, err)
	_, err = store.Append(ctx, "test", "agg1", 1, []es.Envelope{
		{ID: "e2", Type: "TestEvent", AggregateID: "agg1", AggregateType: "test", Version: 2, OccurredAt: testNow},
	})
	require.NoError(t, err)

	// Subscribe starting from seq 2 — should only get events with seq >= 2.
	sub, err := store.Subscribe(ctx,
		es.WithDeliverPolicy(es.DeliverAllPolicy),
		es.WithStartSequence(2),
	)
	require.NoError(t, err)

	select {
	case ev := <-sub.Chan():
		assert.Equal(t, uint64(2), ev.Seq, "should receive event with seq=2")
	case <-time.After(time.Second):
		t.Fatal("did not receive expected event within 1s")
	}

	// No more events should be available.
	select {
	case ev := <-sub.Chan():
		t.Fatalf("unexpected event: seq=%d", ev.Seq)
	case <-time.After(100 * time.Millisecond):
		// expected — no more events
	}
}
