package ds

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

type testMapFactory struct {
	X int `json:"x"`
}

func (f testMapFactory) Create(id string) *testMapFactory { return &testMapFactory{} }

func TestMap_JSON(t *testing.T) {
	t.Run("marshal", func(t *testing.T) {
		m := NewMap[testMapFactory]()
		m.Ensure("foobar").X = 10
		data, err := json.Marshal(m)
		require.NoError(t, err)
		require.Equal(t, `{"foobar":{"x":10}}`, string(data))
	})

	t.Run("unmarshal", func(t *testing.T) {
		var m Map[testMapFactory]
		require.NoError(t, json.Unmarshal([]byte(`{"foobar":{"x":10}}`), &m))
		require.Equal(t, 10, m.Ensure("foobar").X)
	})

	t.Run("unmarshal wrapped nil", func(t *testing.T) {
		type fooType struct {
			M Map[testMapFactory] `json:"m"`
		}
		var f fooType
		require.NoError(t, json.Unmarshal([]byte(`{}`), &f))
		require.NotNil(t, f.M)
	})
}
