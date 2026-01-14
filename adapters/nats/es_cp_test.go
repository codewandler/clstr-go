package nats

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/codewandler/clstr-go/core/es/types"
)

func TestES_Checkpoint(t *testing.T) {
	connectNATS := NewTestContainer(t)

	t.Run("sub checkpoint store", func(t *testing.T) {
		cp, err := NewSubCpStore(SubCpStoreConfig{
			Bucket:  "foo",
			Key:     "dummy",
			Connect: connectNATS,
		})
		require.NoError(t, err)
		require.NotNil(t, cp)

		lastSeq, err := cp.Get()
		require.NoError(t, err)
		require.Equal(t, uint64(0), lastSeq)

		require.NoError(t, cp.Set(123))

		lastSeq, err = cp.Get()
		require.NoError(t, err)
		require.Equal(t, uint64(123), lastSeq)
	})

	t.Run("checkpoint store", func(t *testing.T) {

		cp, err := NewCpStore(CpStoreConfig{
			Bucket:  "foo",
			Connect: connectNATS,
		})
		require.NoError(t, err)
		require.NotNil(t, cp)

		v, err := cp.Get("my_project", "blog-1234")
		require.NoError(t, err)
		require.Equal(t, types.Version(0), v)

		err = cp.Set("my_project", "blog-1234", 123)
		require.NoError(t, err)

		v, err = cp.Get("my_project", "blog-1234")
		require.NoError(t, err)
		require.Equal(t, types.Version(123), v)
	})

}
