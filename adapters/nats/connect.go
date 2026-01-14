package nats

import (
	"os"
	"sync"

	natsgo "github.com/nats-io/nats.go"
)

type Connector func() (*natsgo.Conn, error)

func ReuseConnection(connect Connector) Connector {
	var mu sync.Mutex
	var nc *natsgo.Conn
	return func() (*natsgo.Conn, error) {
		mu.Lock()
		defer mu.Unlock()
		if nc == nil {
			var err error
			nc, err = connect()
			return nc, err
		}
		return nc, nil
	}
}

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
