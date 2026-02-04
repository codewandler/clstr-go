package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

type ClientOptions struct {
	Transport       ClientTransport
	NumShards       uint32
	Seed            string
	EnvelopeOptions []EnvelopeOption
	Metrics         ClusterMetrics
}

type Client struct {
	t         ClientTransport
	numShards uint32
	seed      string
	opts      []EnvelopeOption
	metrics   ClusterMetrics
}

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

func (c *Client) Key(key string, opts ...EnvelopeOption) *ScopedClient {
	return &ScopedClient{
		client: c,
		shard:  c.getShardForKey(key),
		key:    key,
		opts:   append([]EnvelopeOption{WithHeader(envHeaderKey, key)}, opts...),
	}
}

func (c *Client) Shard(shard uint32) *ScopedClient {
	return &ScopedClient{client: c, shard: shard}
}

// === Scoped Client ===

type ScopedClient struct {
	client *Client
	shard  uint32
	key    string
	opts   []EnvelopeOption
}

func (c *ScopedClient) GetNodeInfo(ctx context.Context) (res *GetNodeInfoResponse, err error) {
	return NewRequest[GetNodeInfoRequest, GetNodeInfoResponse](c).Request(ctx, GetNodeInfoRequest{})
}

func (c *ScopedClient) requestRaw(ctx context.Context, msgType string, data []byte, opts ...EnvelopeOption) ([]byte, error) {
	allOpts := append(c.opts, opts...)
	return c.client.RequestShard(ctx, c.shard, msgType, data, allOpts...)
}

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

type (
	Requester interface {
		requestRaw(ctx context.Context, msgType string, data []byte, opts ...EnvelopeOption) ([]byte, error)
	}
)

type (
	Request[IN any, OUT any] struct {
		requester Requester
	}
)

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
