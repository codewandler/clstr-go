package codec

import "encoding/json"

type Codec interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
}

type JSONCodec struct{}

func (JSONCodec) Marshal(v any) ([]byte, error)   { return json.MarshalIndent(v, "", "  ") }
func (JSONCodec) Unmarshal(b []byte, v any) error { return json.Unmarshal(b, v) }
