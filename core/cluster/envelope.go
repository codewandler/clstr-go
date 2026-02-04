package cluster

import (
	"strings"
	"time"

	"github.com/codewandler/clstr-go/core/reflector"
)

const (
	envHeaderKey         = "x-clstr-key"
	reservedHeaderPrefix = "x-clstr-"
)

// EnvelopeOption configures envelope properties before sending.
type EnvelopeOption func(*Envelope)

// WithHeader adds a custom header to the envelope.
// Headers with the "x-clstr-" prefix are reserved and will cause validation to fail.
func WithHeader(key, value string) EnvelopeOption {
	return func(e *Envelope) {
		if e.Headers == nil {
			e.Headers = make(map[string]string)
		}
		e.Headers[key] = value
	}
}

// Envelope is the message container for cluster communication.
// It wraps the payload with routing and metadata information.
type Envelope struct {
	// Shard is the target shard number for routing.
	Shard int `json:"shard"`
	// Type is the message type name for handler dispatch.
	Type string `json:"type"`
	// Data is the JSON-encoded message payload.
	Data []byte `json:"data"`
	// ReplyTo is the reply address (set by transport for request-response).
	ReplyTo string `json:"reply_to,omitempty"`
	// Headers contains custom metadata.
	Headers map[string]string `json:"headers,omitempty"`
	// TTLMs is the time-to-live in milliseconds. Zero means no expiration.
	TTLMs int64 `json:"ttl_ms,omitempty"`
	// CreatedAtMs is the creation timestamp in milliseconds since epoch.
	CreatedAtMs int64 `json:"created_at_ms,omitempty"`
}

// GetHeader retrieves a header value by key.
// Returns empty string and false if the header is not set.
func (e Envelope) GetHeader(key string) (string, bool) {
	if e.Headers == nil {
		return "", false
	}
	v, ok := e.Headers[key]
	return v, ok
}

// Expired returns true if the envelope has a TTL set and has expired.
func (e Envelope) Expired() bool {
	if e.TTLMs <= 0 || e.CreatedAtMs <= 0 {
		return false
	}
	return time.Now().UnixMilli() > e.CreatedAtMs+e.TTLMs
}

// TTL returns the remaining TTL duration. Returns 0 if no TTL is set or already expired.
func (e Envelope) TTL() time.Duration {
	if e.TTLMs <= 0 || e.CreatedAtMs <= 0 {
		return 0
	}
	remaining := e.CreatedAtMs + e.TTLMs - time.Now().UnixMilli()
	if remaining <= 0 {
		return 0
	}
	return time.Duration(remaining) * time.Millisecond
}

// Validate checks the envelope for configuration errors.
// It returns ErrReservedHeader if any header uses the reserved "x-clstr-" prefix.
func (e Envelope) Validate() error {
	for k := range e.Headers {
		if strings.HasPrefix(strings.ToLower(k), reservedHeaderPrefix) && k != envHeaderKey {
			return ErrReservedHeader
		}
	}
	return nil
}

// WithTTL sets the time-to-live for the envelope.
func WithTTL(ttl time.Duration) EnvelopeOption {
	return func(e *Envelope) {
		e.TTLMs = ttl.Milliseconds()
	}
}

func getMessageType(v any) string {
	switch t := v.(type) {
	case interface{ messageType() string }:
		return t.messageType()
	case interface{ MessageType() string }:
		return t.MessageType()
	default:
		return reflector.TypeInfoOf(v).Name
	}
}
