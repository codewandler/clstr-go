package app

import (
	"log/slog"
	"testing"

	"github.com/codewandler/clstr-go/core/actor/v2"
	"github.com/codewandler/clstr-go/core/cluster"
	"github.com/stretchr/testify/require"
)

func TestApp(t *testing.T) {
	slog.SetLogLoggerLevel(slog.LevelInfo)

	// 1. define request/response types
	type (
		ping struct{ Seq int }
		pong struct{ Seq int }
	)

	// 2. create application with handlers
	app, err := Run(
		Config{},
		actor.HandleRequest[ping, pong](func(h actor.HandlerCtx, i ping) (*pong, error) {
			return &pong{Seq: i.Seq + 1}, nil
		}),
	)
	require.NoError(t, err)
	require.NotNil(t, app)

	// 3. use client to send requests
	myClient := app.Client().Key("tenant-1")
	pr, err := cluster.NewRequest[ping, pong](myClient).Request(t.Context(), ping{Seq: 1})
	require.NoError(t, err)
	require.NotNil(t, pr)
	require.Equal(t, 2, pr.Seq)
}
