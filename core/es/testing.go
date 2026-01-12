package es

import (
	"log/slog"
	"testing"
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
	return &TestingEnv{
		t: t,
		Env: NewEnv(
			WithSnapshotter(NewInMemorySnapshotter(slog.Default())),
			WithStore(NewInMemoryStore()),
			WithEnvOpts(opts...),
		),
	}
}
