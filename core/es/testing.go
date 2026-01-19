package es

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// === Helpers ===

type TestingEnv struct {
	*Env
	t *testing.T
}

func (e *TestingEnv) Assert() *TestingEnvAssert {
	return &TestingEnvAssert{env: e}
}

func StartTestEnv(
	t *testing.T,
	opts ...EnvOption,
) *TestingEnv {
	e := NewEnv(
		WithSnapshotter(NewInMemorySnapshotter()),
		WithStore(NewInMemoryStore()),
		WithEnvOpts(opts...),
	)
	require.NoError(t, e.Start())
	return &TestingEnv{
		t:   t,
		Env: e,
	}
}

type TestingEnvAssert struct {
	env *TestingEnv
}

func (t *TestingEnvAssert) Append(
	ctx context.Context,
	expect Version,
	aggType string,
	aggID string,
	events ...any,
) {
	require.NoError(t.env.t, t.env.Append(ctx, aggType, aggID, expect, events...))
}
