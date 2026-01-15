package es

import (
	"time"

	"github.com/codewandler/clstr-go/core/cache"
)

type (
	repoOpts struct {
		snapshotter Snapshotter
		cache       cache.Cache
	}
	RepositoryOption interface{ applyToRepository(*repoOpts) }

	RepoCacheOption valueOption[cache.Cache]
)

type (
	repoSaveOptions struct {
		snapshot    bool
		snapshotTTL time.Duration
	}
	repoLoadOptions struct{ snapshot bool }
	SaveOption      interface{ applyToSaveOptions(*repoSaveOptions) }
	LoadOption      interface{ applyToLoadOptions(*repoLoadOptions) }
)

func WithRepoCache(cache cache.Cache) RepoCacheOption { return RepoCacheOption{v: cache} }
func WithRepoCacheLRU(size int) RepoCacheOption {
	return WithRepoCache(cache.NewLRU(cache.LRUOpts{Size: size}))
}

func (o SnapshotterOption) applyToRepository(options *repoOpts)         { options.snapshotter = o.v }
func (o RepoCacheOption) applyToRepository(options *repoOpts)           { options.cache = o.v }
func (o SnapshotOption) applyToSaveOptions(options *repoSaveOptions)    { options.snapshot = true }
func (o SnapshotTTLOption) applyToSaveOptions(options *repoSaveOptions) { options.snapshotTTL = o.v }
func (o SnapshotOption) applyToLoadOptions(options *repoLoadOptions)    { options.snapshot = true }
