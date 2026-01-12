package actor

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func newTestActor(t *testing.T, hs ...HandlerRegistration) Actor {

	cfg := Options{
		Context:            t.Context(),
		ControlSize:        10_000,
		MailboxSize:        10_000,
		MaxConcurrentTasks: 1000,
	}

	return New(cfg, TypedHandlers(hs...))
}

func TestActor_default(t *testing.T) {
	a := newTestActor(
		t,
		DefaultHandler(func(hc HandlerCtx, msg any) (any, error) {
			s := "Hello"
			return &s, nil
		}),
	)

	res, err := Request[string, string](t.Context(), a, "Hi!")
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, "Hello", *res)
}

func TestActor_simple_request(t *testing.T) {
	type (
		ping struct{ Seq int }
		pong struct{ Seq int }
	)
	a := newTestActor(
		t,
		HandleRequest[ping, pong](func(hc HandlerCtx, ping ping) (*pong, error) {
			return &pong{Seq: ping.Seq + 1}, nil
		}),
	)
	res, err := Request[ping, pong](t.Context(), a, ping{Seq: 1})
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, 2, res.Seq)
}

func TestActor_publish(t *testing.T) {
	type msg struct{ V int }
	ch := make(chan msg, 1)
	a := newTestActor(
		t,
		HandleMsg[msg](func(hc HandlerCtx, msg msg) error {
			ch <- msg
			return nil
		}),
	)

	require.NoError(t, Publish(t.Context(), a, msg{V: 42}))

	select {
	case <-time.After(time.Second):
		t.Fatal("timeout")
	case <-ch:
	}
}

func TestActor_publish_err(t *testing.T) {
	type msg struct{ V int }
	a := newTestActor(
		t,
		HandleMsg[msg](func(hc HandlerCtx, msg msg) error {
			return fmt.Errorf("uups")
		}),
	)

	require.ErrorContains(t, Publish(t.Context(), a, msg{V: 42}), "uups")

}
