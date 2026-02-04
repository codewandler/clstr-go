package cluster

import "errors"

var (
	// Transport errors
	ErrTransportClosed            = errors.New("transport closed")
	ErrTransportNoShardSubscriber = errors.New("no subscriber for shard")

	// Envelope errors
	ErrEnvelopeExpired = errors.New("envelope TTL expired")
	ErrReservedHeader  = errors.New("cannot set reserved header")

	// Handler errors
	ErrHandlerTimeout   = errors.New("handler exceeded deadline")
	ErrKeyRequired      = errors.New("key is required")
	ErrMissingKeyHeader = errors.New("missing x-clstr-key header")
)
