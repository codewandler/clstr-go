package ds

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSet_Json(t *testing.T) {
	s := NewStringSet("hello", "world", "!")

	var data []byte

	data, _ = json.Marshal(&s)
	require.Equal(t, `["hello","world","!"]`, string(data))

	data, _ = json.Marshal(s)
	require.Equal(t, `["hello","world","!"]`, string(data))

	data, _ = json.Marshal(*s)
	require.Equal(t, `["hello","world","!"]`, string(data))
}

func TestSet_AddRemove(t *testing.T) {
	s := NewStringSet()
	require.True(t, s.IsEmpty())

	s.Add("hello")
	require.False(t, s.IsEmpty())

	s.Remove("hello")
	require.True(t, s.IsEmpty())
}

func TestSet_Removals(t *testing.T) {
	a := NewSet[string]("g1")
	b := NewSet[string]("g1")
	c := a.Removals(b)
	require.True(t, c.IsEmpty())
}

func TestSet_Intersect_Basic(t *testing.T) {
	a := NewStringSet("a", "b", "c")
	b := NewStringSet("b", "c", "d")

	i := a.Intersect(b)
	require.ElementsMatch(t, []string{"b", "c"}, i.Values())
}

func TestSet_Intersect_Order_By_Left_Set(t *testing.T) {
	left := NewStringSet("x", "a", "b", "c", "y")
	right := NewStringSet("c", "a", "z")

	// Intersection should be elements present in both, ordered by the left set order
	i := left.Intersect(right)
	require.Equal(t, []string{"a", "c"}, i.Values())

	// Reversing should keep set membership the same but order follows the new left set
	j := right.Intersect(left)
	require.Equal(t, []string{"c", "a"}, j.Values())
}

func TestSet_Intersect_EmptyOrDisjoint(t *testing.T) {
	empty := NewStringSet()
	some := NewStringSet("a", "b")
	disjointA := NewStringSet("x", "y")

	require.True(t, empty.Intersect(some).IsEmpty())
	require.True(t, some.Intersect(empty).IsEmpty())
	require.True(t, some.Intersect(disjointA).IsEmpty())
}

func TestDiffs(t *testing.T) {
	s1 := NewStringSet("a", "b", "c")
	s2 := NewStringSet("b", "c", "d")
	require.Equal(t, []string{"d"}, s1.Additions(s2).Values())
}

func TestSet_Diff(t *testing.T) {
	cur := NewStringSet("a", "b", "c")
	other := NewStringSet("b", "c", "d", "e")

	add, remove := cur.Diff(other)

	// add: elements in other but not in cur, ordered by other's insertion order
	require.Equal(t, []string{"d", "e"}, add.Values())

	// remove: elements in cur but not in other, ordered by cur's insertion order
	require.Equal(t, []string{"a"}, remove.Values())

	// No-ops when equal
	a2 := NewStringSet("x", "y")
	b2 := NewStringSet("x", "y")
	add2, remove2 := a2.Diff(b2)
	require.True(t, add2.IsEmpty())
	require.True(t, remove2.IsEmpty())
}

func TestContains_Any(t *testing.T) {
	s1 := NewSet("t1", "t2", "t3")
	require.True(t, s1.ContainsAny(NewSet("t2", "t4")))
}
