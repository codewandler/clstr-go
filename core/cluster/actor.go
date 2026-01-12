package cluster

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/codewandler/clstr-go/core/actor/v2"
)

type ActorFactory func(key string) (actor.Actor, error)

func NewActorHandler(actFactory ActorFactory) ServerHandlerFunc {

	return NewKeyHandler(func(key string) (ServerHandlerFunc, error) {
		// create actor
		act, err := actFactory(key)
		if err != nil {
			return nil, err
		}

		// return handler
		return func(ctx context.Context, env Envelope) (data []byte, err error) {

			var res any
			res, err = actor.RawRequest(ctx, act, env.Type, env.Data)
			if err != nil {
				return nil, fmt.Errorf("failed to send message to actor: %w", err)
			}

			data, err = json.Marshal(res)
			return
		}, nil
	})
}
