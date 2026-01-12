package reflector

import (
	"reflect"
	"sync"
)

var (
	muCache sync.RWMutex
	cache   = make(map[reflect.Type]TypeInfo)
)

type TypeInfo struct {
	Name string
	Type reflect.Type
}

func TypeInfoOf(x any) TypeInfo {
	return TypeInfoForType(reflect.TypeOf(x))
}

func TypeInfoFor[T any]() TypeInfo {
	return TypeInfoForType(reflect.TypeOf((*T)(nil)).Elem())
}

func TypeInfoForType(t reflect.Type) TypeInfo {
	// check cache
	muCache.RLock()
	ti, ok := cache[t]
	muCache.RUnlock()
	if ok {
		return ti
	}

	// lookup
	if t == nil {
		return TypeInfo{}
	}
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}

	ti = TypeInfo{
		Name: t.PkgPath() + "." + t.Name(),
		Type: t,
	}

	muCache.Lock()
	cache[t] = ti
	muCache.Unlock()
	return ti
}
