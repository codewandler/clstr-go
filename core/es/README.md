# Event Sourcing (es)

*Detailed documentation coming soon.*

For now, see the [main README](../../README.md) for an overview.

## Quick Reference

```go
// Create an aggregate
type User struct {
    es.BaseAggregate
    email string
}

// Raise events
func (u *User) ChangeEmail(email string) {
    u.Raise(&EmailChanged{Email: email})
}

// Apply events to rebuild state
func (u *User) Apply(event es.Event) {
    switch e := event.Data.(type) {
    case *EmailChanged:
        u.email = e.Email
    }
}

// Use with repository
repo := es.NewTypedRepositoryFrom[*User](log, env.Repository())
user, _ := repo.GetByID(ctx, userID)
user.ChangeEmail("new@example.com")
repo.Save(ctx, user)
```

## Key Types

- `Aggregate` — Domain object interface
- `BaseAggregate` — Embeddable base implementation
- `EventStore` — Event persistence interface
- `Repository` — Application-level aggregate operations
- `Consumer` — Event stream processor with checkpointing
- `Environment` — Factory bundling store, repository, and consumers
