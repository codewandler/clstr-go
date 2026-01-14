package nats

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNats_Connect(t *testing.T) {

	connect := NewTestContainer(t)
	nc1, err := connect()
	require.NoError(t, err)
	require.NotNil(t, nc1)
	require.Equal(t, "CONNECTED", nc1.Status().String())

	nc2, err := connect()
	require.NoError(t, err)
	require.NotNil(t, nc2)
	require.Equal(t, "CONNECTED", nc2.Status().String())

	require.Equal(t, nc1, nc2)

	nc1.Close()
	nc2.Close()
}
