package cluster

import (
	"context"
)

// Subscription represents an active shard subscription that can be unsubscribed.
type Subscription interface {
	// Unsubscribe stops receiving messages for the subscribed shard.
	Unsubscribe() error
}

// ServerHandlerFunc is the callback for processing incoming shard messages.
// It receives the envelope and returns a response or error.
type ServerHandlerFunc = func(ctx context.Context, env Envelope) ([]byte, error)

// ClientTransport is the interface for sending messages to the cluster.
type ClientTransport interface {
	// Request sends a message and waits for a reply.
	// Returns the response data or an error (e.g., [ErrTransportNoShardSubscriber]).
	Request(ctx context.Context, env Envelope) ([]byte, error)

	// Close releases transport resources.
	Close() error
}

// ServerTransport is the interface for receiving messages from the cluster.
type ServerTransport interface {
	// SubscribeShard registers a handler for messages targeting the given shard.
	// Returns a subscription that can be used to unsubscribe.
	SubscribeShard(ctx context.Context, shardID uint32, h ServerHandlerFunc) (Subscription, error)

	// Close releases transport resources and stops all subscriptions.
	Close() error
}

// Transport combines client and server transport capabilities.
// Implementations typically wrap a messaging system like NATS.
type Transport interface {
	ClientTransport
	ServerTransport
}
