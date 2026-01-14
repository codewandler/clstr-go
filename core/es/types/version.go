package types

import "log/slog"

type Version uint64

func (v Version) Uint64() uint64                         { return uint64(v) }
func (v Version) SlogAttr() slog.Attr                    { return newSlogVersionAttr("version", v) }
func (v Version) SlogAttrWithKey(key string) slog.Attr   { return newSlogVersionAttr(key, v) }
func newSlogVersionAttr(key string, v Version) slog.Attr { return slog.Uint64(key, uint64(v)) }
