// Package actor provides a mailbox-based actor model implementation for
// building concurrent, message-driven systems.
//
// Actors are the fundamental unit of computation in this package. Each actor:
//   - Has a unique identity
//   - Processes messages sequentially from its mailbox
//   - Can schedule background tasks via [HandlerCtx.Schedule]
//   - Can be paused, resumed, and stepped for debugging/testing
//
// # Creating Actors
//
// The simplest way to create an actor is using typed handlers:
//
//	actor := actor.TypedHandlers(
//	    actor.HandleMsg[CreateUserCmd](func(hc actor.HandlerCtx, cmd CreateUserCmd) error {
//	        // Handle the command
//	        return nil
//	    }),
//	    actor.HandleRequest[GetUserQuery, *User](func(hc actor.HandlerCtx, q GetUserQuery) (*User, error) {
//	        // Handle request and return response
//	        return &User{ID: q.ID}, nil
//	    }),
//	    actor.HandleEvery(time.Minute, func(hc actor.HandlerCtx) error {
//	        // Periodic task executed every minute
//	        return nil
//	    }),
//	).ToActor(actor.Options{})
//
// # Message Handling
//
// Messages are dispatched by type name to registered handlers:
//
//   - [HandleMsg] registers a one-way message handler (fire-and-forget)
//   - [HandleRequest] registers a request-response handler
//   - [HandleEvery] registers a periodic task that runs at fixed intervals
//   - [DefaultHandler] registers a fallback for unmatched message types
//   - [Init] registers initialization logic run when the actor starts
//
// # Sending Messages
//
// Use [Request] for request-response patterns:
//
//	user, err := actor.Request[GetUserQuery, *User](ctx, myActor, GetUserQuery{ID: "123"})
//
// Use [Publish] for fire-and-forget messages:
//
//	err := actor.Publish[CreateUserCmd](ctx, myActor, CreateUserCmd{Name: "Alice"})
//
// # Background Tasks
//
// Handlers can schedule background work via [HandlerCtx.Schedule]:
//
//	actor.HandleMsg[ProcessDataCmd](func(hc actor.HandlerCtx, cmd ProcessDataCmd) error {
//	    hc.Schedule(func() {
//	        // This runs asynchronously, outside the actor's mailbox processing
//	        processInBackground(cmd.Data)
//	    })
//	    return nil
//	})
//
// The actor waits for all scheduled tasks to complete during shutdown.
//
// # Self-Request Detection
//
// The actor automatically detects when a handler attempts to send a request
// back to itself (which would deadlock) and returns [ErrSelfRequest].
//
// # Lifecycle Control
//
// Actors support pause/resume for debugging and testing:
//
//	actor.Pause()       // Stop processing messages
//	actor.Step()        // Process exactly one message
//	actor.Resume()      // Continue normal processing
//	<-actor.Done()      // Wait for actor shutdown
package actor
