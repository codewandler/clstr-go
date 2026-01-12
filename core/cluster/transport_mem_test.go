package cluster

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTransport_Memory(t *testing.T) {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	tr := NewInMemoryTransport()
	rcv := make(chan Envelope, 1)
	s, err := tr.SubscribeShard(t.Context(), 5, func(ctx context.Context, envelope Envelope) ([]byte, error) {
		rcv <- envelope
		return nil, nil
	})
	require.NoError(t, err)
	require.NotNil(t, s)

	_, err = tr.Request(t.Context(), Envelope{
		Shard: 5,
		Data:  []byte("hello"),
		Type:  "banana",
	})
	require.NoError(t, err)

	select {
	case <-time.After(10 * time.Millisecond):
		t.Fatal("no message received")
	case env := <-rcv:
		require.Equal(t, "hello", string(env.Data))
	}

	require.NoError(t, s.Unsubscribe())
	require.NoError(t, tr.Close())

}

func TestTransport_Memory_publish_error(t *testing.T) {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	tr := NewInMemoryTransport().WithLog(slog.Default())
	s, err := tr.SubscribeShard(t.Context(), 5, func(ctx context.Context, envelope Envelope) ([]byte, error) {
		return nil, errors.New("boom")
	})
	require.NoError(t, err)
	require.NotNil(t, s)

	_, err = tr.Request(t.Context(), Envelope{
		Shard: 5,
		Data:  []byte("hello"),
		Type:  "banana",
	})
	require.ErrorContains(
		t,
		err,
		"boom",
	)

	<-time.After(10 * time.Millisecond)

	require.NoError(t, s.Unsubscribe())
	require.NoError(t, tr.Close())

}
