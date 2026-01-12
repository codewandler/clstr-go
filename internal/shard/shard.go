package shard

import "hash/fnv"

type Func func(key string) int

func ForKey(key string, shardCount int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(shardCount))
}

type Sharder interface {
	GetShardForKey(key string) int
}

type fnSharder struct {
	fn Func
}

func NewSharder(fn Func) Sharder {
	return &fnSharder{fn: fn}
}

func (s *fnSharder) GetShardForKey(key string) int { return s.fn(key) }

func Distributed(count int) Sharder {
	return &fnSharder{
		fn: func(key string) int {
			return ForKey(key, count)
		},
	}
}

func Const(shard int) Sharder {
	return &fnSharder{
		fn: func(key string) int {
			return shard
		},
	}
}
