package cluster

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestEnvelope_TTL_Expired(t *testing.T) {
	now := time.Now().UnixMilli()

	// No TTL set - not expired
	env := Envelope{}
	require.False(t, env.Expired())
	require.Equal(t, time.Duration(0), env.TTL())

	// TTL set but no CreatedAtMs - not expired
	env = Envelope{TTLMs: 1000}
	require.False(t, env.Expired())
	require.Equal(t, time.Duration(0), env.TTL())

	// CreatedAtMs set but no TTL - not expired
	env = Envelope{CreatedAtMs: now}
	require.False(t, env.Expired())
	require.Equal(t, time.Duration(0), env.TTL())

	// TTL in the future - not expired
	env = Envelope{
		TTLMs:       1000,
		CreatedAtMs: now,
	}
	require.False(t, env.Expired())
	require.Greater(t, env.TTL(), time.Duration(0))

	// TTL in the past - expired
	env = Envelope{
		TTLMs:       100,
		CreatedAtMs: now - 200, // 200ms ago, TTL was 100ms
	}
	require.True(t, env.Expired())
	require.Equal(t, time.Duration(0), env.TTL())
}

func TestEnvelope_Validate_ReservedHeaders(t *testing.T) {
	// Empty headers - valid
	env := Envelope{}
	require.NoError(t, env.Validate())

	// Regular header - valid
	env = Envelope{
		Headers: map[string]string{
			"my-header": "value",
		},
	}
	require.NoError(t, env.Validate())

	// envHeaderKey (x-clstr-key) is allowed
	env = Envelope{
		Headers: map[string]string{
			envHeaderKey: "some-key",
		},
	}
	require.NoError(t, env.Validate())

	// Other x-clstr-* headers are reserved
	env = Envelope{
		Headers: map[string]string{
			"x-clstr-internal": "value",
		},
	}
	require.ErrorIs(t, env.Validate(), ErrReservedHeader)

	// Case insensitive check
	env = Envelope{
		Headers: map[string]string{
			"X-CLSTR-INTERNAL": "value",
		},
	}
	require.ErrorIs(t, env.Validate(), ErrReservedHeader)
}

func TestWithTTL(t *testing.T) {
	env := Envelope{}
	WithTTL(5 * time.Second)(&env)

	require.Equal(t, int64(5000), env.TTLMs)
}
