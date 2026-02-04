package es

import "log/slog"

// Version represents the version number of an aggregate within its stream.
// It is a monotonically increasing value starting from 1 for the first event.
// Version is used for optimistic concurrency control - when saving changes,
// the expected version must match the current version in the store.
type Version uint64

func (v Version) Uint64() uint64                         { return uint64(v) }
func (v Version) SlogAttr() slog.Attr                    { return newSlogVersionAttr("version", v) }
func (v Version) SlogAttrWithKey(key string) slog.Attr   { return newSlogVersionAttr(key, v) }
func newSlogVersionAttr(key string, v Version) slog.Attr { return slog.Uint64(key, uint64(v)) }
