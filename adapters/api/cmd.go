package api

import (
	"fmt"

	"github.com/codewandler/clstr-go/internal/reflector"
)

type ExecuteCommandRequestBody[PAYLOAD any] struct {
	Data PAYLOAD `json:"data"` // Data is the payload of the command
}

func (c ExecuteCommandRequestBody[PAYLOAD]) RequestPayload() any { return c.Data }

func (c ExecuteCommandRequestBody[PAYLOAD]) getPayloadMessageType() string {
	var pl PAYLOAD
	if x, ok := any(pl).(interface{ MessageType() string }); ok {
		return x.MessageType()
	}
	return reflector.TypeInfoFor[PAYLOAD]().Name
}

func (c ExecuteCommandRequestBody[PAYLOAD]) MsgType() string {
	return fmt.Sprintf("cmd:request(%s)", c.getPayloadMessageType())
}
