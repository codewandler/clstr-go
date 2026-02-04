package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

// ClientOptions configures client creation.
type ClientOptions struct {
	// Transport is the underlying messaging transport (required).
	Transport ClientTransport
	// NumShards is the total number of shards in the cluster (required).
	NumShards uint32
	// Seed is an optional string for consistent shard hashing across clients.
	Seed string
	// EnvelopeOptions are default options applied to all outgoing envelopes.
	EnvelopeOptions []EnvelopeOption
	// Metrics for client instrumentation. If nil, a no-op implementation is used.
	Metrics ClusterMetrics
}

// Client routes messages to cluster nodes based on shard assignment.
// Create via [NewClient].
type Client struct {
	t         ClientTransport
	numShards uint32
	seed      string
	opts      []EnvelopeOption
	metrics   ClusterMetrics
}

// NewClient creates a new cluster client with the given options.
// Returns an error if required options (Transport, NumShards) are missing.
func NewClient(opts ClientOptions) (*Client, error) {
	if opts.Transport == nil {
		return nil, fmt.Errorf("cluster: ClientOptions.Transport is required")
	}
	if opts.NumShards == 0 {
		return nil, fmt.Errorf("cluster: ClientOptions.NumShards is required")
	}
	metrics := opts.Metrics
	if metrics == nil {
		metrics = NopClusterMetrics()
	}
	return &Client{
		t:         opts.Transport,
		numShards: opts.NumShards,
		seed:      opts.Seed,
		metrics:   metrics,
	}, nil
}

// getShardForKey derives a stable shard (0..NumShards-1) from an arbitrary string key.
func (c *Client) getShardForKey(key string) uint32 {
	return ShardFromString(key, c.numShards, c.seed)
}

func (c *Client) newEnv(shard uint32, msgType string, data []byte, opts ...EnvelopeOption) (Envelope, error) {
	e := Envelope{
		Shard: int(shard),
		Type:  msgType,
		Data:  data,
	}
	for _, opt := range c.opts {
		opt(&e)
	}
	for _, opt := range opts {
		opt(&e)
	}
	if err := e.Validate(); err != nil {
		return Envelope{}, err
	}
	return e, nil
}

// recordTransportError maps known transport errors to metric labels.
func (c *Client) recordTransportError(err error) {
	if err == nil {
		return
	}
	switch {
	case errors.Is(err, ErrTransportNoShardSubscriber):
		c.metrics.TransportError("no_subscriber")
	case errors.Is(err, ErrHandlerTimeout):
		c.metrics.TransportError("timeout")
	case errors.Is(err, ErrEnvelopeExpired):
		c.metrics.TransportError("ttl_expired")
	case errors.Is(err, ErrTransportClosed):
		c.metrics.TransportError("closed")
	}
}

// NotifyShard publishes directly to a shard (caller already knows shard).
func (c *Client) NotifyShard(ctx context.Context, shard uint32, msgType string, data []byte, opts ...EnvelopeOption) error {
	if shard >= c.numShards {
		return fmt.Errorf("cluster: shard %d out of range (numShards=%d)", shard, c.numShards)
	}
	env, err := c.newEnv(shard, msgType, data, opts...)
	if err != nil {
		return err
	}
	_, err = c.t.Request(ctx, env)
	c.metrics.NotifyCompleted(msgType, err == nil)
	if err != nil {
		c.recordTransportError(err)
	}
	return err
}

// RequestShard sends a request directly to a shard.
func (c *Client) RequestShard(ctx context.Context, shard uint32, msgType string, data []byte, opts ...EnvelopeOption) ([]byte, error) {
	if shard >= c.numShards {
		return nil, fmt.Errorf("cluster: shard %d out of range (numShards=%d)", shard, c.numShards)
	}
	env, err := c.newEnv(shard, msgType, data, opts...)
	if err != nil {
		return nil, err
	}

	// instrument
	defer c.metrics.RequestDuration(msgType).ObserveDuration()

	result, err := c.t.Request(ctx, env)
	c.metrics.RequestCompleted(msgType, err == nil)
	if err != nil {
		c.recordTransportError(err)
	}
	return result, err
}

// NotifyKey publishes using a string key -> shard mapping (e.g. tenantID, userID, deviceID).
func (c *Client) NotifyKey(ctx context.Context, key string, msgType string, data []byte) error {
	shard := c.getShardForKey(key)
	return c.NotifyShard(ctx, shard, msgType, data)
}

// RequestKey requests using a string key -> shard mapping.
func (c *Client) RequestKey(ctx context.Context, key string, msgType string, data []byte, opts ...EnvelopeOption) ([]byte, error) {
	shard := c.getShardForKey(key)
	return c.RequestShard(ctx, shard, msgType, data, opts...)
}

// Key returns a client scoped to the shard determined by the given key.
// The key is hashed consistently to determine the target shard.
// Use this for entity-based routing (e.g., by user ID, tenant ID).
func (c *Client) Key(key string, opts ...EnvelopeOption) *ScopedClient {
	return &ScopedClient{
		client: c,
		shard:  c.getShardForKey(key),
		key:    key,
		opts:   append([]EnvelopeOption{WithHeader(envHeaderKey, key)}, opts...),
	}
}

// Shard returns a client scoped to a specific shard number.
// Use this when you already know the target shard.
func (c *Client) Shard(shard uint32) *ScopedClient {
	return &ScopedClient{client: c, shard: shard}
}

// ScopedClient is a client bound to a specific shard, enabling fluent
// request/notify operations without repeatedly specifying the shard or key.
type ScopedClient struct {
	client *Client
	shard  uint32
	key    string
	opts   []EnvelopeOption
}

// GetNodeInfo queries the node that owns this shard for its metadata.
func (c *ScopedClient) GetNodeInfo(ctx context.Context) (res *GetNodeInfoResponse, err error) {
	return NewRequest[GetNodeInfoRequest, GetNodeInfoResponse](c).Request(ctx, GetNodeInfoRequest{})
}

func (c *ScopedClient) requestRaw(ctx context.Context, msgType string, data []byte, opts ...EnvelopeOption) ([]byte, error) {
	allOpts := append(c.opts, opts...)
	return c.client.RequestShard(ctx, c.shard, msgType, data, allOpts...)
}

// Request sends a request to the scoped shard and waits for the response.
// The payload is JSON-encoded and the message type is derived from the payload type.
func (c *ScopedClient) Request(ctx context.Context, payload any, opts ...EnvelopeOption) ([]byte, error) {

	if err := validatePayload(payload); err != nil {
		return nil, err
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return c.requestRaw(ctx, getMessageType(payload), data, opts...)
}

// Notify sends a fire-and-forget message to the scoped shard.
func (c *ScopedClient) Notify(ctx context.Context, msg any, opts ...EnvelopeOption) error {
	if err := validatePayload(msg); err != nil {
		return err
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	allOpts := append(c.opts, opts...)
	return c.client.NotifyShard(ctx, c.shard, getMessageType(msg), data, allOpts...)
}

// Requester is implemented by types that can send raw requests.
type Requester interface {
	requestRaw(ctx context.Context, msgType string, data []byte, opts ...EnvelopeOption) ([]byte, error)
}

// Request is a typed request builder for making type-safe cluster requests.
type Request[IN any, OUT any] struct {
	requester Requester
}

// NewRequest creates a typed request builder for the given requester.
// Use this for type-safe request/response patterns.
func NewRequest[IN any, OUT any](requester Requester) *Request[IN, OUT] {
	return &Request[IN, OUT]{
		requester: requester,
	}
}

func validatePayload(payload any) error {
	if v, ok := payload.(interface{ Validate() error }); ok {
		err := v.Validate()
		if err != nil {
			return fmt.Errorf("invalid payload: %w", err)
		}
	}
	return nil
}

// Request sends the payload and deserializes the response into *OUT.
func (r *Request[IN, OUT]) Request(ctx context.Context, payload IN) (out *OUT, err error) {
	if err = validatePayload(payload); err != nil {
		return nil, err
	}

	// marshall to JSON
	var data []byte
	data, err = json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	// send request
	data, err = r.requester.requestRaw(ctx, getMessageType(payload), data)
	if err != nil {
		return nil, err
	}
	out = new(OUT)
	err = json.Unmarshal(data, out)
	return
}
