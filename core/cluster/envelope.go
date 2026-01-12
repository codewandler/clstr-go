package cluster

import "github.com/codewandler/clstr-go/internal/reflector"

const (
	envHeaderKey = "x-clstr-key"
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
	Shard   int               `json:"shard"`
	Type    string            `json:"type"`
	Data    []byte            `json:"data"`
	ReplyTo string            `json:"reply_to,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
}

func (e Envelope) GetHeader(key string) (string, bool) {
	if e.Headers == nil {
		return "", false
	}
	v, ok := e.Headers[key]
	return v, ok
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
