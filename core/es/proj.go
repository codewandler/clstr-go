package es

type (

	// Projection consumes persisted events to build read models / indexes.
	Projection interface {
		Name() string
		Handler
	}
)
