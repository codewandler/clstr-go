package es

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"time"

	"github.com/codewandler/clstr-go/core/cache"
	gonanoid "github.com/matoous/go-nanoid/v2"
)

type (
	Repository interface {
		Load(ctx context.Context, agg Aggregate, opts ...LoadOption) error
		Save(ctx context.Context, agg Aggregate, opts ...SaveOption) error
		CreateSnapshot(ctx context.Context, agg Aggregate, saveSnapshotOpts SnapshotSaveOpts) (Snapshot, error)
	}
)

// Repository rehydrates aggregates and persists new events with optimistic concurrency.
type repository struct {
	log         *slog.Logger
	store       EventStore
	registry    *EventRegistry
	snapshotter Snapshotter
}

func newRepoOpts(opts ...RepositoryOption) repoOpts {
	var options = repoOpts{
		cache: cache.NewNop(),
	}
	for _, opt := range opts {
		opt.applyToRepository(&options)
	}
	return options
}

func NewRepository(
	log *slog.Logger,
	store EventStore,
	registry *EventRegistry,
	opts ...RepositoryOption,
) Repository {
	options := newRepoOpts(opts...)

	r := &repository{
		log:         log.With(slog.String("repo", fmt.Sprintf("%T", store))),
		store:       store,
		registry:    registry,
		snapshotter: options.snapshotter,
	}

	return r
}

// Load rehydrates agg from the store and sets GetID/version.
func (r *repository) Load(ctx context.Context, agg Aggregate, opts ...LoadOption) (err error) {
	// validate
	aggType := agg.GetAggType()
	if aggType == "" {
		return errors.New("aggregate type is empty")
	}
	aggID := agg.GetID()
	if aggID == "" {
		return errors.New("aggregate id is empty")
	}
	if len(agg.Uncommitted()) != 0 {
		return errors.New("aggregate has uncommitted events (dirty=true)")
	}

	// populate load options
	loadOptions := repoLoadOptions{}
	for _, opt := range opts {
		opt.applyToLoadOptions(&loadOptions)
	}

	/*log := r.log.With(
		slog.Group(
			"agg",
			slog.String("type", aggType),
			slog.String("id", aggID),
			//slog.Uint64("seq", curSeq),
			//slog.Int("version", curVersion),
		),
	)

	log.Debug("loading")*/

	// load from snapshot
	if loadOptions.snapshot {
		if r.snapshotter == nil {
			return ErrSnapshotterUnconfigured
		}
		err = ApplySnapshot(ctx, r.snapshotter, agg)
		if err != nil {
			if !errors.Is(err, ErrSnapshotNotFound) {
				return fmt.Errorf("failed to apply snapshot: %w", err)
			}
		} else {
			/*log.Debug(
				"snapshot applied",
				slog.Uint64("seq", agg.GetSeq()),
				agg.GetVersion().SlogAttr(),
			)*/
		}
	}

	var (
		curVersion = agg.GetVersion()
		curSeq     = agg.GetSeq()
		minVersion = curVersion + 1
		minSeq     = curSeq + 1
	)

	/*log = r.log.With(
		slog.Group(
			"agg",
			slog.String("type", aggType),
			slog.String("id", aggID),
			slog.Uint64("seq", curSeq),
			curVersion.SlogAttr(),
		),
	)*/

	/*log.Debug(
		"load",
		slog.Group("opts",
			slog.Uint64("min_seq", minSeq),
			minVersion.SlogAttrWithKey("min_version"),
			slog.Bool("snapshot", loadOptions.snapshot),
		),
	)*/

	// load all events
	loaded, err := r.store.Load(
		ctx,
		aggType,
		aggID,
		WithStartAtVersion(minVersion),
		WithStartAtSeq(minSeq),
	)
	if err != nil {
		return err
	}

	// apply all events
	for _, e := range loaded {

		expectVersion := agg.GetVersion() + 1
		if e.Version != expectVersion {
			return fmt.Errorf("expect version %d, got %d", expectVersion, e.Version)
		}

		evt, err := r.registry.Decode(e)
		if err != nil {
			return err
		}
		if err := agg.Apply(evt); err != nil {
			return err
		}

		// update version & sequence
		agg.setVersion(e.Version)
		agg.setSeq(e.Seq)
		curVersion = e.Version
		curSeq = e.Seq
	}

	if curVersion == 0 {
		return fmt.Errorf("failed to load agg_type=%s agg_id=%s: %w", aggType, aggID, ErrAggregateNotFound)
	}

	return nil
}

func (r *repository) Save(ctx context.Context, agg Aggregate, saveOpts ...SaveOption) error {
	uncommitted := agg.Uncommitted()
	if len(uncommitted) == 0 {
		return nil
	}
	aggType := agg.GetAggType()
	if aggType == "" {
		return errors.New("aggregate type is empty")
	}
	aggID := agg.GetID()
	if aggID == "" {
		return errors.New("aggregate id is empty")
	}

	saveOptions := repoSaveOptions{}
	for _, opt := range saveOpts {
		opt.applyToSaveOptions(&saveOptions)
	}

	expectVersion := agg.GetVersion()
	newEnvs := make([]Envelope, 0)
	v := expectVersion

	for _, ev := range uncommitted {
		data, err := json.Marshal(ev)
		if err != nil {
			return err
		}

		v++

		env := Envelope{
			// TODO: allow ID generator
			ID:            gonanoid.Must(),
			Type:          getEventTypeOf(ev),
			AggregateID:   aggID,
			AggregateType: aggType,
			Version:       v,
			OccurredAt:    time.Now(),
			Data:          data,
		}

		err = env.Validate()
		if err != nil {
			return err
		}

		newEnvs = append(newEnvs, env)
	}

	// append to store
	if res, err := r.store.Append(
		ctx,
		aggType,
		aggID,
		expectVersion,
		newEnvs,
	); err != nil {
		return fmt.Errorf("failed to save agg_type=%s agg_id=%s: %w", aggType, aggID, err)
	} else if res != nil {
		agg.setSeq(res.LastSeq)
	} else {
		return errors.New("append returned nil result")
	}

	agg.setVersion(v)
	agg.ClearUncommitted()

	// create snapshot
	if saveOptions.snapshot {
		if _, snapshotErr := r.CreateSnapshot(ctx, agg, SnapshotSaveOpts{TTL: saveOptions.snapshotTTL}); snapshotErr != nil {
			return snapshotErr
		}
	}

	r.log.Debug(
		"saved",
		slog.Group(
			"agg",
			slog.String("id", aggID),
			slog.String("type", aggType),
			slog.Uint64("seq", agg.GetSeq()),
			agg.GetVersion().SlogAttr(),
		),
		slog.Any("opts", saveOptions),
		slog.Int("num_events", len(newEnvs)),
	)

	return nil
}

func (r *repository) CreateSnapshot(ctx context.Context, agg Aggregate, saveOpts SnapshotSaveOpts) (ss Snapshot, err error) {
	if r.snapshotter == nil {
		return ss, ErrSnapshotterUnconfigured
	}
	ss, err = CreateSnapshot(agg)
	if err != nil {
		return ss, fmt.Errorf("failed to create snapshot: %w", err)
	}
	err = r.snapshotter.SaveSnapshot(ctx, ss, saveOpts)
	if err != nil {
		return ss, fmt.Errorf("failed to save snapshot: %w", err)
	}
	r.log.Debug("snapshot saved", ss.logAttrs())
	return
}

var _ Repository = &repository{}

// === TypedRepository ===

type (
	TypedRepository[T Aggregate] interface {
		GetAggType() string
		New() T
		NewWithID(id string) T
		GetOrCreate(ctx context.Context, aggID string, opts ...LoadOption) (T, error)

		// GetByID gets an aggregate by ID. If the aggregate does not exist, it is created.
		GetByID(ctx context.Context, aggID string, opts ...LoadOption) (T, error)

		Save(ctx context.Context, agg T, opts ...SaveOption) error
	}
)

type typedRepo[T Aggregate] struct {
	r     Repository
	log   *slog.Logger
	cache cache.TypedCache[T]
}

func (t *typedRepo[T]) New() T { return t.NewWithID("") }

func (t *typedRepo[T]) NewWithID(id string) T {
	var a T
	if c, ok := any(a).(interface{ Create() T }); ok {
		a = c.Create()
	} else {
		rt := reflect.TypeOf((*T)(nil)).Elem()
		if rt.Kind() == reflect.Pointer {
			a = reflect.New(rt.Elem()).Interface().(T)
		} else {
			a = *new(T)
		}
	}
	a.SetID(id)
	return a
}

func (t *typedRepo[T]) GetOrCreate(ctx context.Context, aggID string, opts ...LoadOption) (a T, err error) {
	if aggID == "" {
		return a, errors.New("aggregate id is empty")
	}
	a = t.NewWithID(aggID)
	err = t.r.Load(ctx, a, opts...)
	if err != nil {
		if errors.Is(err, ErrAggregateNotFound) {
			err = a.Create(aggID)
			if err != nil {
				return a, err
			}
			err = t.Save(ctx, a, WithSnapshot(true))
			if err != nil {
				return a, err
			}

			t.log.Debug("created", slog.String("id", aggID))
		} else {
			return a, err
		}
	}
	return a, nil
}

func (t *typedRepo[T]) GetByID(ctx context.Context, aggID string, opts ...LoadOption) (a T, err error) {
	if aggID == "" {
		return a, errors.New("aggregate id is empty")
	}

	// get from cache
	cached, ok := t.cache.Get(aggID)
	if ok {
		return cached, nil
	}

	a = t.NewWithID(aggID)
	err = t.r.Load(ctx, a, opts...)
	if err != nil {
		return
	}

	// put to cache
	t.cache.Put(aggID, a)

	return a, nil
}

func (t *typedRepo[T]) Save(ctx context.Context, agg T, opts ...SaveOption) (err error) {
	err = t.r.Save(ctx, agg, opts...)
	if err != nil {
		return err
	}
	t.cache.Put(agg.GetID(), agg)
	return nil
}

func (t *typedRepo[T]) GetAggType() string {
	a := t.New()
	return a.GetAggType()
}

func NewTypedRepository[T Aggregate](log *slog.Logger, s EventStore, reg *EventRegistry, opts ...RepositoryOption) TypedRepository[T] {
	return NewTypedRepositoryFrom[T](log, NewRepository(log, s, reg), opts...)
}

func NewTypedRepositoryFrom[T Aggregate](log *slog.Logger, r Repository, opts ...RepositoryOption) TypedRepository[T] {
	options := newRepoOpts(opts...)
	return &typedRepo[T]{
		r:     r,
		log:   log.With(slog.String("repo", fmt.Sprintf("%T", *new(T)))),
		cache: cache.NewTyped[T](options.cache),
	}
}
