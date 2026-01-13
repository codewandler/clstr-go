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

	go func() {
		for i := 0; i < 10; i++ {
			_, _ = json.Marshal(s)
		}
	}()

	s.Process(inc, inc, inc)

	data, err := json.Marshal(s)
	require.NoError(t, err)
	require.Equal(t, `{"value":45}`, string(data))

	v := Read[Data, int](s, func(d *Data) int { return d.Value })
	require.Equal(t, 45, v)
}
