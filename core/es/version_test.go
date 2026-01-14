package es

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVersion(t *testing.T) {
	v1, v2 := Version(1), Version(2)
	require.True(t, v1 < v2)
	require.True(t, v2 > v1)
	require.Equal(t, v1, Version(1))

	data, err := json.Marshal(v1)
	require.NoError(t, err)
	require.Equal(t, `1`, string(data))

	var x Version
	require.NoError(t, json.Unmarshal([]byte("1234"), &x))
	require.Equal(t, Version(1234), x)
}
