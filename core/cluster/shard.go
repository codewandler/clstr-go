package cluster

import (
	"encoding/binary"
	"fmt"
	"sort"

	"golang.org/x/crypto/blake2b"
)

func ShardFromString(key string, numShards uint32, seed string) uint32 {
	if numShards == 0 {
		return 0
	}
	h, _ := blake2b.New(8, nil)
	if seed != "" {
		h.Write([]byte(seed))
		h.Write([]byte{0})
	}
	h.Write([]byte(key))
	sum := h.Sum(nil)
	v := binary.BigEndian.Uint64(sum)
	return uint32(v % uint64(numShards))
}

func ShardsForNode(nodeID string, nodeIDs []string, numShards uint32, seed string) []uint32 {
	if numShards == 0 || len(nodeIDs) == 0 {
		return nil
	}

	nodes := append([]string(nil), nodeIDs...)
	sort.Strings(nodes)

	owned := make([]uint32, 0, numShards/uint32(len(nodes))+1)

	for shard := uint32(0); shard < numShards; shard++ {
		key := fmt.Sprintf("shard:%d", shard) // good key, no binary zeros

		best := ""
		var bestScore uint64

		for _, n := range nodes {
			score := hrwScore64([]byte(key), n, seed)
			if best == "" || score > bestScore {
				best = n
				bestScore = score
			}
		}

		if best == nodeID {
			owned = append(owned, shard)
		}
	}

	return owned
}

func hrwScore64(key []byte, nodeID string, seed string) uint64 {
	h, _ := blake2b.New(8, nil)

	if seed != "" {
		h.Write([]byte(seed))
		h.Write([]byte{0})
	}

	h.Write(key)
	h.Write([]byte{0})
	h.Write([]byte(nodeID))

	sum := h.Sum(nil)
	return binary.BigEndian.Uint64(sum)
}
