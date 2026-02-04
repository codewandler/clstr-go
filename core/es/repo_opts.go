package es

import (
	"time"

	gonanoid "github.com/matoous/go-nanoid/v2"

	"github.com/codewandler/clstr-go/core/cache"
)

// IDGenerator is a function that generates unique IDs for events.
type IDGenerator func() string

// DefaultIDGenerator returns the default ID generator using nanoid.
func DefaultIDGenerator() IDGenerator {
	return func() string { return gonanoid.Must() }
}

type (
	repoOpts struct {
		snapshotter Snapshotter
		cache       cache.Cache
		saveOpts    []SaveOption
		loadOpts    []LoadOption
		idGenerator IDGenerator
		metrics     ESMetrics
	}

	repoSaveOptions struct {
		snapshot    bool
		snapshotTTL time.Duration
		useCache    bool
	}

	repoLoadOptions struct {
		snapshot bool
		useCache bool
	}

	repoLoadAndSaveOpts struct {
		loadOpts []LoadOption
		saveOpts []SaveOption
	}

	repoWithTransactionOpts struct {
		create bool
		repoLoadAndSaveOpts
	}
)

type (
	RepositoryOption      interface{ applyToRepository(*repoOpts) }
	RepoCacheOption       valueOption[cache.Cache]
	RepoCreateOption      valueOption[bool]
	RepoUseCacheOption    valueOption[bool]
	SaveOptsOption        MultiOption[SaveOption]
	LoadOptsOption        MultiOption[LoadOption]
	RepoIDGeneratorOption valueOption[IDGenerator]
)

type (
	SaveOption            interface{ applyToSaveOptions(*repoSaveOptions) }
	LoadOption            interface{ applyToLoadOptions(*repoLoadOptions) }
	LoadAndSaveOption     interface{ applyToLoadAndSaveOptions(*repoLoadAndSaveOpts) }
	WithTransactionOption interface {
		applyToWithTransactionOptions(*repoWithTransactionOpts)
	}
)

func WithCreate() RepoCreateOption                    { return RepoCreateOption{v: true} }
func WithRepoCache(cache cache.Cache) RepoCacheOption { return RepoCacheOption{v: cache} }
func WithRepoCacheLRU(size int) RepoCacheOption {
	return WithRepoCache(cache.NewLRU(cache.LRUOpts{Size: size}))
}

// WithIDGenerator sets a custom ID generator for event envelope IDs.
func WithIDGenerator(gen IDGenerator) RepoIDGeneratorOption {
	return RepoIDGeneratorOption{v: gen}
}

// === repo ==

func (o SnapshotterOption) applyToRepository(options *repoOpts)     { options.snapshotter = o.v }
func (o RepoCacheOption) applyToRepository(options *repoOpts)       { options.cache = o.v }
func (o RepoIDGeneratorOption) applyToRepository(options *repoOpts) { options.idGenerator = o.v }
func (o SaveOptsOption) applyToRepository(options *repoOpts) {
	options.saveOpts = append(options.saveOpts, o.opts...)
}
func (o LoadOptsOption) applyToRepository(options *repoOpts) {
	options.loadOpts = append(options.loadOpts, o.opts...)
}

func newRepoOpts(opts ...RepositoryOption) repoOpts {
	var options = repoOpts{
		cache:       cache.NewNop(),
		snapshotter: NewInMemorySnapshotter(),
		saveOpts:    []SaveOption{WithUseCache(true)},
		loadOpts:    []LoadOption{WithUseCache(true)},
		idGenerator: DefaultIDGenerator(),
	}
	for _, opt := range opts {
		opt.applyToRepository(&options)
	}
	return options
}

// === save ==

func (o SnapshotOption) applyToSaveOptions(options *repoSaveOptions)     { options.snapshot = true }
func (o SnapshotTTLOption) applyToSaveOptions(options *repoSaveOptions)  { options.snapshotTTL = o.v }
func (o RepoUseCacheOption) applyToSaveOptions(options *repoSaveOptions) { options.useCache = o.v }
func (o SaveOptsOption) applyToSaveOptions(options *repoSaveOptions) {
	for _, opt := range o.opts {
		opt.applyToSaveOptions(options)
	}
}
func WithSaveOpts(opts ...SaveOption) SaveOptsOption { return SaveOptsOption{opts: opts} }
func WithUseCache(useCache bool) RepoUseCacheOption  { return RepoUseCacheOption{v: useCache} }

func newSaveOptions(opts ...SaveOption) repoSaveOptions {
	options := repoSaveOptions{}
	for _, opt := range opts {
		opt.applyToSaveOptions(&options)
	}
	return options
}

// === load ==

func (o SnapshotOption) applyToLoadOptions(options *repoLoadOptions)     { options.snapshot = true }
func (o RepoUseCacheOption) applyToLoadOptions(options *repoLoadOptions) { options.useCache = o.v }
func (o LoadOptsOption) applyToLoadOptions(options *repoLoadOptions) {
	for _, opt := range o.opts {
		opt.applyToLoadOptions(options)
	}
}
func WithLoadOpts(opts ...LoadOption) LoadOptsOption { return LoadOptsOption{opts: opts} }

func newLoadOptions(opts ...LoadOption) repoLoadOptions {
	options := repoLoadOptions{}
	for _, opt := range opts {
		opt.applyToLoadOptions(&options)
	}
	return options
}

// === getOrCreate ==

func (o SnapshotOption) applyToLoadAndSaveOptions(options *repoLoadAndSaveOpts) {
	options.loadOpts = append(options.loadOpts, o)
	options.saveOpts = append(options.saveOpts)
}

func (o RepoUseCacheOption) applyToLoadAndSaveOptions(options *repoLoadAndSaveOpts) {
	options.loadOpts = append(options.loadOpts, o)
}

func (o LoadOptsOption) applyToLoadAndSaveOptions(options *repoLoadAndSaveOpts) {
	options.loadOpts = append(options.loadOpts, o.opts...)
}

func (o SaveOptsOption) applyToLoadAndSaveOptions(options *repoLoadAndSaveOpts) {
	options.saveOpts = append(options.saveOpts, o.opts...)
}

func newGetOrCreateOptions(opts ...LoadAndSaveOption) repoLoadAndSaveOpts {
	options := repoLoadAndSaveOpts{}
	for _, opt := range opts {
		opt.applyToLoadAndSaveOptions(&options)
	}
	return options
}

// === withTransaction ==

func (o SaveOptsOption) applyToWithTransactionOptions(options *repoWithTransactionOpts) {
	options.saveOpts = append(options.saveOpts, o.opts...)
}
func (o LoadOptsOption) applyToWithTransactionOptions(options *repoWithTransactionOpts) {
	options.loadOpts = append(options.loadOpts, o.opts...)
}
func (o SnapshotOption) applyToWithTransactionOptions(options *repoWithTransactionOpts) {
	options.saveOpts = append(options.saveOpts, WithSnapshot(o.v))
	options.loadOpts = append(options.loadOpts, WithSnapshot(o.v))
}
func (o SnapshotTTLOption) applyToWithTransactionOptions(options *repoWithTransactionOpts) {
	options.saveOpts = append(options.saveOpts, WithSnapshotTTL(o.v))
}
func (o RepoUseCacheOption) applyToWithTransactionOptions(options *repoWithTransactionOpts) {
	options.saveOpts = append(options.saveOpts, WithUseCache(o.v))
	options.loadOpts = append(options.loadOpts, WithUseCache(o.v))
}

func (o RepoCreateOption) applyToWithTransactionOptions(options *repoWithTransactionOpts) {
	options.create = o.v
}

func newWithTransactionOptions(opts ...WithTransactionOption) repoWithTransactionOpts {
	options := repoWithTransactionOpts{}
	for _, opt := range opts {
		opt.applyToWithTransactionOptions(&options)
	}
	return options
}
