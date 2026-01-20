package nats

import (
	"errors"
	"io"
	"log/slog"
	"os"
	"time"

	natsgo "github.com/nats-io/nats.go"
)

type closeFunc = func()

type Connector func() (nc *natsgo.Conn, err error)

var natsLog = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
	Level: slog.LevelWarn,
}))

var connectOpts = []natsgo.Option{
	natsgo.MaxReconnects(-1),
	natsgo.ReconnectWait(2 * time.Second),
	natsgo.Timeout(2 * time.Second),
	natsgo.DisconnectErrHandler(func(nc *natsgo.Conn, err error) {
		if errors.Is(err, io.EOF) {
			natsLog.Debug("disconnected", slog.Any("error", err))
			return
		}
		natsLog.Warn("disconnected", slog.Any("error", err))
	}),
	natsgo.ReconnectHandler(func(nc *natsgo.Conn) {
		natsLog.Info("reconnected")
	}),
	natsgo.ClosedHandler(func(nc *natsgo.Conn) {
		natsLog.Info("closed")
	}),
}

func ConnectURL(natsURL string) Connector {
	return func() (*natsgo.Conn, error) {
		nc, err := natsgo.Connect(
			natsURL,
			connectOpts...,
		)
		if err != nil {
			return nil, err
		}
		return nc, nil
	}
}

func ConnectDefault() Connector {
	if natsURL := os.Getenv("NATS_URL"); natsURL != "" {
		return ConnectURL(natsURL)
	}
	return ConnectURL(natsgo.DefaultURL)
}
