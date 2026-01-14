package nats

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNats_Connect(t *testing.T) {

	connect := NewTestContainer(t)
	nc1, disconnect1, err := connect()
	require.NoError(t, err)
	require.NotNil(t, nc1)
	require.Equal(t, "CONNECTED", nc1.Status().String())

	nc2, disconnect2, err := connect()
	require.NoError(t, err)
	require.NotNil(t, nc2)
	require.Equal(t, "CONNECTED", nc2.Status().String())

	disconnect1()
	disconnect2()

	require.Equal(t, "CLOSED", nc1.Status().String())

	nc3, _, err := connect()
	require.NoError(t, err)
	require.NotNil(t, nc3)
	require.Equal(t, "CONNECTED", nc3.Status().String())

}
