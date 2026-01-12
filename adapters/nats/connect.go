package nats

import (
	"os"

	natsgo "github.com/nats-io/nats.go"
)

type Connector func() (*natsgo.Conn, error)

func ConnectURL(natsURL string) Connector {
	return func() (*natsgo.Conn, error) {
		return natsgo.Connect(
			natsURL,
			natsgo.MaxReconnects(3),
		)
	}
}

func ConnectDefault() Connector {
	if natsURL := os.Getenv("NATS_URL"); natsURL != "" {
		return ConnectURL(natsURL)
	}
	return ConnectURL(natsgo.DefaultURL)
}
