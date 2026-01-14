package es

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
)

// === Helpers ===

type TestingEnv struct {
	*Env
	t *testing.T
}

func NewTestEnv(
	t *testing.T,
	opts ...EnvOption,
) *TestingEnv {
	e, err := NewEnv(
		WithSnapshotter(NewInMemorySnapshotter(slog.Default())),
		WithStore(NewInMemoryStore()),
		WithEnvOpts(opts...),
	)
	require.NoError(t, err)
	return &TestingEnv{
		t:   t,
		Env: e,
	}
}
