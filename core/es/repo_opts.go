package es

import (
	"time"

	"github.com/codewandler/clstr-go/core/cache"
)

type (
	repoOpts struct {
		snapshotter Snapshotter
		cache       cache.Cache
		saveOpts    []SaveOption
		loadOpts    []LoadOption
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

	repoGetOrCreateOptions struct {
		loadOpts []LoadOption
		saveOpts []SaveOption
	}
)

type (
	RepositoryOption   interface{ applyToRepository(*repoOpts) }
	RepoCacheOption    valueOption[cache.Cache]
	RepoUseCacheOption valueOption[bool]
	SaveOptsOption     MultiOption[SaveOption]
	LoadOptsOption     MultiOption[LoadOption]
)

type (
	SaveOption        interface{ applyToSaveOptions(*repoSaveOptions) }
	LoadOption        interface{ applyToLoadOptions(*repoLoadOptions) }
	GetOrCreateOption interface{ applyToGetOrCreateOptions(*repoGetOrCreateOptions) }
)

func WithRepoCache(cache cache.Cache) RepoCacheOption { return RepoCacheOption{v: cache} }
func WithRepoCacheLRU(size int) RepoCacheOption {
	return WithRepoCache(cache.NewLRU(cache.LRUOpts{Size: size}))
}

// === repo ==

func (o SnapshotterOption) applyToRepository(options *repoOpts) { options.snapshotter = o.v }
func (o RepoCacheOption) applyToRepository(options *repoOpts)   { options.cache = o.v }
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
func WithSaveOpts(opts ...SaveOption) SaveOption    { return SaveOptsOption{opts: opts} }
func WithUseCache(useCache bool) RepoUseCacheOption { return RepoUseCacheOption{v: useCache} }

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
func WithLoadOpts(opts ...LoadOption) LoadOption { return LoadOptsOption{opts: opts} }

func newLoadOptions(opts ...LoadOption) repoLoadOptions {
	options := repoLoadOptions{}
	for _, opt := range opts {
		opt.applyToLoadOptions(&options)
	}
	return options
}

// === getOrCreate ==

func (o SnapshotOption) applyToGetOrCreateOptions(options *repoGetOrCreateOptions) {
	options.loadOpts = append(options.loadOpts, o)
	options.saveOpts = append(options.saveOpts)
}

func (o RepoUseCacheOption) applyToGetOrCreateOptions(options *repoGetOrCreateOptions) {
	options.loadOpts = append(options.loadOpts, o)
}

func newGetOrCreateOptions(opts ...GetOrCreateOption) repoGetOrCreateOptions {
	options := repoGetOrCreateOptions{}
	for _, opt := range opts {
		opt.applyToGetOrCreateOptions(&options)
	}
	return options
}
