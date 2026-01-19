package actor

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestState_JSON(t *testing.T) {
	type Data struct {
		Value int `json:"value"`
	}
	s := NewState[Data](
		t.Context(),
		&Data{Value: 42},
		func(d *Data) {},
	)
	inc := func(d *Data) { d.Value++ }

	// test concurrent marshalling
	go func() {
		for i := 0; i < 10; i++ {
			_, _ = json.Marshal(s)
		}
	}()

	// test read
	v := Read[Data, int](s, func(d *Data) int { return d.Value })
	require.Equal(t, 42, v)

	// increment 3 times
	s.Process(inc, inc, inc)

	v = Read[Data, int](s, func(d *Data) int { return d.Value })
	require.Equal(t, 45, v)

	data, err := json.Marshal(s)
	require.NoError(t, err)
	require.Equal(t, `{"value":45}`, string(data))

	require.NoError(t, json.Unmarshal([]byte(`{"value": 23}`), s))
	require.Equal(t, 23, Read[Data, int](s, func(d *Data) int { return d.Value }))
	s.Process(func(d *Data) {
		require.Equal(t, 23, d.Value)
	})

}
