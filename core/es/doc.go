// Package es provides an event sourcing framework for building event-driven applications.
//
// # Overview
//
// Event sourcing is a pattern where application state is stored as a sequence of events
// rather than as current state snapshots. This package provides the core abstractions
// and implementations for building event-sourced systems in Go.
//
// # Core Components
//
// The package provides several key components:
//
// Aggregate: The domain object that encapsulates business logic and state changes.
// Events are raised within aggregates and applied to update internal state.
// Use [BaseAggregate] as an embeddable helper that tracks version and uncommitted events.
//
//	type User struct {
//	    es.BaseAggregate
//	    Name  string
//	    Email string
//	}
//
//	func (u *User) ChangeName(name string) error {
//	    return es.RaiseAndApply(u, &NameChanged{Name: name})
//	}
//
// EventStore: The persistence layer for events. It provides [EventStore.Load] to retrieve
// events for an aggregate and [EventStore.Append] to persist new events with optimistic
// concurrency control. Use [NewInMemoryStore] for testing or implement the interface
// for production storage (e.g., NATS JetStream via the adapters/nats package).
//
// Repository: The application-level interface for working with aggregates.
// It handles loading aggregates by replaying events and saving new events.
// Use [NewTypedRepository] for type-safe operations with generics:
//
//	repo := es.NewTypedRepository[*User](log, store, registry)
//	user, err := repo.GetByID(ctx, "user-123")
//	user.ChangeName("New Name")
//	repo.Save(ctx, user)
//
// Consumer: Processes events from the store for building read models or triggering
// side effects. Supports checkpointing for exactly-once semantics and live mode
// detection to distinguish historical replay from real-time events:
//
//	consumer := es.NewConsumer(store, registry, handler,
//	    es.WithConsumerName("user-projector"),
//	    es.WithMiddlewares(es.NewCheckpointMiddleware(checkpointStore)),
//	)
//	consumer.Start(ctx)
//
// # Event Registration
//
// Events must be registered with an [EventRegistry] before they can be decoded:
//
//	registry := es.NewEventRegistry()
//	registry.Register(es.Event[NameChanged]())
//	registry.Register(es.Event[EmailChanged]())
//
// # Snapshots
//
// For aggregates with many events, snapshots can optimize loading by capturing
// state at a point in time. Implement [Snapshottable] for custom serialization,
// or let the framework use JSON marshaling as a fallback:
//
//	// Load with snapshot optimization
//	user, err := repo.GetByID(ctx, "user-123", es.WithSnapshot(true))
//
//	// Save with snapshot
//	repo.Save(ctx, user, es.WithSnapshot(true))
//
// # Concurrency Control
//
// The framework uses optimistic concurrency via the [Version] type. When saving,
// the repository checks that the aggregate's version matches the store's version.
// If another process has modified the aggregate, [ErrConcurrencyConflict] is returned.
//
// For serialized access to a single aggregate, use [TypedRepository.WithTransaction]:
//
//	repo.WithTransaction(ctx, "user-123", func(user *User) error {
//	    return user.ChangeName("New Name")
//	})
//
// # Environment
//
// The [Environment] type provides a factory for creating configured instances
// of stores, repositories, and consumers with shared configuration:
//
//	env := es.NewEnvironment(
//	    es.WithLog(logger),
//	    es.WithStore(natsStore),
//	    es.WithEvent[NameChanged](),
//	    es.WithAggregates(&User{}),
//	)
//	repo := env.Repository()
//	consumer := env.NewConsumer(handler)
package es
