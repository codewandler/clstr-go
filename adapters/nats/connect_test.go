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

	nc1.Close()
	nc2.Close()

	require.Equal(t, "CLOSED", nc1.Status().String())

	nc3, err := connect()
	require.NoError(t, err)
	require.NotNil(t, nc3)
	require.Equal(t, "CONNECTED", nc3.Status().String())

}
