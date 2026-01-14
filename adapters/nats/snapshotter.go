package nats

import (
	"context"
	"errors"
	"fmt"

	"github.com/codewandler/clstr-go/core/es"
)

type Snapshotter struct {
	kv *KvStore[es.Snapshot]
}

func (s *Snapshotter) getKey(ss *es.Snapshot) string {
	return fmt.Sprintf("%s-%s", ss.ObjType, ss.ObjID)
}

func (s *Snapshotter) SaveSnapshot(ctx context.Context, snapshot *es.Snapshot) error {
	return s.kv.Set(ctx, s.getKey(snapshot), *snapshot)
}

func (s *Snapshotter) LoadSnapshot(ctx context.Context, objType, objID string) (*es.Snapshot, error) {
	ss, err := s.kv.Get(ctx, s.getKey(&es.Snapshot{ObjType: objType, ObjID: objID}))
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			return nil, es.ErrSnapshotNotFound
		}
		err = fmt.Errorf(
			"failed to load snapshot for %s-%s: %w",
			objType, objID, err,
		)
		return nil, err
	}
	return &ss, nil
}

func NewSnapshotter(cfg KvConfig) (*Snapshotter, error) {
	kv, err := NewKvStore[es.Snapshot](cfg)
	if err != nil {
		return nil, err
	}
	return &Snapshotter{kv: kv}, nil
}

var _ es.Snapshotter = (*Snapshotter)(nil)
