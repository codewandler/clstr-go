# Actor Model (actor)

*Detailed documentation coming soon.*

For now, see the [main README](../../README.md) for an overview.

## Quick Reference

```go
// Define a handler
type EchoHandler struct{}

func (h *EchoHandler) Handle(ctx *actor.HandlerCtx, msg *Echo) (*Echo, error) {
    return msg, nil
}

// Create and start an actor
a := actor.New(
    actor.WithHandler[*Echo, *Echo](&EchoHandler{}),
    actor.WithMailboxSize(1024),
)
a.Start(ctx)
defer a.Shutdown()

// Send a request
response, err := actor.Request[*Echo](ctx, a, &Echo{Message: "hello"})
```

## Key Types

- `BaseActor` — Core actor with mailbox-based message processing
- `HandlerCtx` — Context passed to handlers with Request() and Schedule()
- `TypedHandlerRegistry` — Type-safe message routing
- `Scheduler` — Bounded concurrency for background tasks

## Features

- Mailbox isolation (no shared state)
- Type-safe request/response
- Panic containment
- Step mode for testing
- Self-request deadlock prevention
