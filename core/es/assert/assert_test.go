package assert

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAssert(t *testing.T) {
	mustBeTrue := True(true, "must be true")
	require.True(t, mustBeTrue.Eval())
	require.NoError(t, mustBeTrue.Check())
	require.NoError(t, mustBeTrue.Check())
	require.Equal(t, "must be true", mustBeTrue.String())

	mustBeFalse := False(false, "must be false")
	require.True(t, mustBeFalse.Eval())
	require.NoError(t, mustBeFalse.Check())
	require.Equal(t, "must be false", mustBeFalse.String())

	require.NoError(t, All(mustBeTrue, mustBeFalse).Check())

	require.Error(t, All(mustBeTrue, mustBeFalse, newCond("foo", func() bool {
		return false
	})).Check())
	require.Error(t, All(mustBeTrue, mustBeFalse, newCond("foo", func() bool {
		return false
	})).Check())

}
