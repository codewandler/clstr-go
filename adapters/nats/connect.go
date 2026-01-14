package nats

import (
	"os"
	"sync"
	"sync/atomic"

	natsgo "github.com/nats-io/nats.go"
)

type closeFunc = func()

type Connector func() (nc *natsgo.Conn, close closeFunc, err error)

func ReuseConnection(connect Connector) Connector {
	var mu sync.Mutex
	var nc *natsgo.Conn
	var closeCon closeFunc
	var leased atomic.Int64
	var weakClose closeFunc = func() {
		mu.Lock()
		defer mu.Unlock()
		if leased.Add(-1) == 0 {
			closeCon()
			nc = nil
		}
	}
	return func() (*natsgo.Conn, closeFunc, error) {
		mu.Lock()
		defer mu.Unlock()
		if nc == nil {
			var err error
			nc, closeCon, err = connect()
			if err != nil {
				return nil, nil, err
			}
			leased.Add(1)
			return nc, weakClose, nil
		}
		leased.Add(1)
		return nc, weakClose, nil
	}
}

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
