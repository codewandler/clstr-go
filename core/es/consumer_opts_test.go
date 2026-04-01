package es

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// noopMW is a no-op HandlerMiddleware for option tests.
var noopMW HandlerMiddleware = func(next Handler) Handler { return next }

// TestWithMiddlewaresReplaces verifies that WithMiddlewares replaces any
// previously configured middlewares with the new list.
func TestWithMiddlewaresReplaces(t *testing.T) {
	opts := newConsumerOpts(
		WithMiddlewares(noopMW, noopMW), // set two
		WithMiddlewares(noopMW),         // replace with one
	)
	require.Len(t, opts.mws, 1,
		"second WithMiddlewares call should replace, not extend, the list")
}

// TestWithMiddlewaresAppendExtends verifies that WithMiddlewaresAppend adds to
// the existing list without discarding the previously configured middlewares.
func TestWithMiddlewaresAppendExtends(t *testing.T) {
	opts := newConsumerOpts(
		WithMiddlewares(noopMW),         // set one
		WithMiddlewaresAppend(noopMW, noopMW), // append two
	)
	require.Len(t, opts.mws, 3,
		"WithMiddlewaresAppend should extend, not replace, the existing list")
}
