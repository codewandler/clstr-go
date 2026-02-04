package ds

import (
	"fmt"
	"testing"
)

func BenchmarkSet_Add(b *testing.B) {
	for _, size := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				s := NewSet[int]()
				for j := 0; j < size; j++ {
					s.Add(j)
				}
			}
		})
	}
}

func BenchmarkSet_Contains(b *testing.B) {
	for _, size := range []int{10, 100, 1000} {
		s := NewSet[int]()
		for j := 0; j < size; j++ {
			s.Add(j)
		}
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = s.Contains(size / 2) // hit
				_ = s.Contains(size + 1) // miss
			}
		})
	}
}

func BenchmarkSet_Remove_Single(b *testing.B) {
	for _, size := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			// Pre-allocate all sets
			sets := make([]*Set[int], b.N)
			for i := range sets {
				s := NewSet[int]()
				for j := 0; j < size; j++ {
					s.Add(j)
				}
				sets[i] = s
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				sets[i].Remove(size / 2) // remove from middle
			}
		})
	}
}

func BenchmarkSet_Remove_Batch(b *testing.B) {
	for _, size := range []int{100, 1000} {
		for _, removeCount := range []int{10, 50} {
			b.Run(fmt.Sprintf("size=%d/remove=%d", size, removeCount), func(b *testing.B) {
				toRemove := make([]int, removeCount)
				for i := range toRemove {
					toRemove[i] = i * (size / removeCount)
				}
				// Pre-allocate all sets
				sets := make([]*Set[int], b.N)
				for i := range sets {
					s := NewSet[int]()
					for j := 0; j < size; j++ {
						s.Add(j)
					}
					sets[i] = s
				}
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					sets[i].Remove(toRemove...)
				}
			})
		}
	}
}

func BenchmarkSet_ForEach(b *testing.B) {
	for _, size := range []int{10, 100, 1000} {
		s := NewSet[int]()
		for j := 0; j < size; j++ {
			s.Add(j)
		}
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				sum := 0
				s.ForEach(func(v int) {
					sum += v
				})
			}
		})
	}
}

func BenchmarkSet_Values(b *testing.B) {
	for _, size := range []int{10, 100, 1000} {
		s := NewSet[int]()
		for j := 0; j < size; j++ {
			s.Add(j)
		}
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = s.Values()
			}
		})
	}
}

func BenchmarkSet_Intersect(b *testing.B) {
	for _, size := range []int{10, 100, 1000} {
		s1 := NewSet[int]()
		s2 := NewSet[int]()
		for j := 0; j < size; j++ {
			s1.Add(j)
			s2.Add(j + size/2) // 50% overlap
		}
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = s1.Intersect(s2)
			}
		})
	}
}

func BenchmarkSet_IsSubsetOf(b *testing.B) {
	for _, size := range []int{10, 100, 1000} {
		subset := NewSet[int]()
		superset := NewSet[int]()
		for j := 0; j < size; j++ {
			superset.Add(j)
			if j%2 == 0 {
				subset.Add(j)
			}
		}
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = subset.IsSubsetOf(superset)
			}
		})
	}
}
