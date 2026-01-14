package nats

import (
	"os"

	natsgo "github.com/nats-io/nats.go"
)

type closeFunc = func()

type Connector func() (nc *natsgo.Conn, close closeFunc, err error)

func ConnectURL(natsURL string) Connector {
	return func() (*natsgo.Conn, closeFunc, error) {
		nc, err := natsgo.Connect(
			natsURL,
			natsgo.MaxReconnects(3),
		)
		if err != nil {
			return nil, nil, err
		}
		return nc, func() { nc.Close() }, nil
	}
}

func ConnectDefault() Connector {
	if natsURL := os.Getenv("NATS_URL"); natsURL != "" {
		return ConnectURL(natsURL)
	}
	return ConnectURL(natsgo.DefaultURL)
}
