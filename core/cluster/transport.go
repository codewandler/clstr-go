package cluster

import (
	"context"
	"errors"
)

var (
	ErrTransportClosed            = errors.New("transport closed")
	ErrTransportNoShardSubscriber = errors.New("no subscriber for shard")
)

type Subscription interface {
	Unsubscribe() error
}

type ServerHandlerFunc = func(ctx context.Context, env Envelope) ([]byte, error)

type ClientTransport interface {
	// Request sends a message and waits for a reply.
	Request(ctx context.Context, env Envelope) ([]byte, error)

	Close() error
}

type ServerTransport interface {
	// SubscribeShard delivers envelopes for the shard.
	SubscribeShard(ctx context.Context, shardID uint32, h ServerHandlerFunc) (Subscription, error)

	Close() error
}

// Transport sends messages and lets you subscribe for shards you "own".
type Transport interface {
	ClientTransport
	ServerTransport
}
