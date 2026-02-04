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

type EnvelopeOption func(*Envelope)

func WithHeader(key, value string) EnvelopeOption {
	return func(e *Envelope) {
		if e.Headers == nil {
			e.Headers = make(map[string]string)
		}
		e.Headers[key] = value
	}
}

type Envelope struct {
	Shard       int               `json:"shard"`
	Type        string            `json:"type"`
	Data        []byte            `json:"data"`
	ReplyTo     string            `json:"reply_to,omitempty"`
	Headers     map[string]string `json:"headers,omitempty"`
	TTLMs       int64             `json:"ttl_ms,omitempty"`
	CreatedAtMs int64             `json:"created_at_ms,omitempty"`
}

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
