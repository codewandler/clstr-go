package domain

import (
	"encoding/json"
	"fmt"

	"github.com/codewandler/clstr-go/core/es"
)

type (
	TestAgg struct {
		es.BaseAggregate

		Counter        uint16 `json:"counter"`
		NumIncrements  int    `json:"num_increments"`
		NumResets      int    `json:"num_resets"`
		NumTotalEvents int    `json:"num_total_events"`
	}

	Incremented struct {
		Inc   uint8 `json:"inc,omitempty"`
		Reset bool  `json:"reset,omitempty"`
	}
)

func (a *TestAgg) Snapshot() (data []byte, err error) { return json.Marshal(a) }
func (a *TestAgg) RestoreSnapshot(data []byte) error  { return json.Unmarshal(data, a) }
func (a *TestAgg) GetAggType() string                 { return "test_agg" }
func (a *TestAgg) Register(r es.Registrar)            { es.RegisterEvents(r, es.Event[Incremented]()) }
func (a *TestAgg) Apply(event any) error {

	switch e := event.(type) {
	case *Incremented:
		a.NumTotalEvents++

		if e.Inc > 0 {
			a.Counter += uint16(e.Inc)
			a.NumIncrements += 1
		}

		if e.Reset {
			a.Counter = 0
			a.NumResets++
		}

		return nil
	}
	return fmt.Errorf("unknown event: %T", event)
}

var _ es.Snapshottable = &TestAgg{}

// === Commands ===

func (a *TestAgg) Reset() error { return es.RaiseAndApply(a, &Incremented{Reset: true}) }
func (a *TestAgg) Inc() error   { return a.IncBy(1) }
func (a *TestAgg) IncBy(v uint8) error {
	if a.Counter+uint16(v) > 24 {
		return fmt.Errorf("counter cannot exceed 24")
	}
	return es.RaiseAndApply(a, &Incremented{Inc: v})
}

// === Read ===

func (a *TestAgg) Count() int {
	return int(a.Counter)
}

func NewTestAgg(id string) *TestAgg {
	a := &TestAgg{}
	a.SetID(id)
	return a
}
