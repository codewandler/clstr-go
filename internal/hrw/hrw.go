package hrw

import (
	"encoding/binary"
	"sort"

	"golang.org/x/crypto/blake2b"
)

// TopK returns up to k nodes with the highest Rendezvous/HRW scores for key.
// seed is optional (e.g., cluster GetID) to avoid cross-cluster collisions.
func TopK(key string, nodes []string, k int, seed string) []string {
	if k <= 0 || len(nodes) == 0 {
		return nil
	}
	if k > len(nodes) {
		k = len(nodes)
	}

	type entry struct {
		score uint64
		idx   int
	}
	top := make([]entry, 0, k)

	keyB := []byte(key)

	for i := range nodes {
		id := nodes[i]
		s := hrwScore64(keyB, id, seed)

		if len(top) < k {
			top = append(top, entry{score: s, idx: i})
			if len(top) == k {
				// keep smallest at top[0] by sorting ascending (k is usually small)
				sort.Slice(top, func(a, b int) bool { return top[a].score < top[b].score })
			}
			continue
		}

		// top[0] is current smallest score among selected top-k
		if s > top[0].score {
			top[0] = entry{score: s, idx: i}
			// restore ascending order (k is small, re-sort is fine)
			sort.Slice(top, func(a, b int) bool { return top[a].score < top[b].score })
		}
	}

	// Return in descending score order (best first)
	sort.Slice(top, func(a, b int) bool { return top[a].score > top[b].score })

	out := make([]string, len(top))
	for i, e := range top {
		out[i] = nodes[e.idx]
	}
	return out
}

// Best returns the single best node (top-1). ok=false if nodes is empty.
func Best(key string, nodes []string, seed string) (best string, ok bool) {
	if len(nodes) == 0 {
		return best, false
	}
	out := TopK(key, nodes, 1, seed)
	return out[0], true
}

func hrwScore64(key []byte, nodeID string, seed string) uint64 {
	// 8-byte digest => uint64 score
	h, _ := blake2b.New(8, nil)

	// Optional personalization via including seed in the input.
	// (Go's blake2b supports "config" personalization only via x/crypto; this is fine.)
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
