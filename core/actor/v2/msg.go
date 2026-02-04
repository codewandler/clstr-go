package actor

import "github.com/codewandler/clstr-go/core/reflector"

type msgTyper interface{ MsgType() string }

func msgTypeFor[T any]() string {
	var z T
	if mt, ok := any(z).(msgTyper); ok {
		return mt.MsgType()
	}
	return reflector.TypeInfoFor[T]().Name

}

func msgTypeOf(x any) string {
	if mt, ok := x.(msgTyper); ok {
		return mt.MsgType()
	}
	return reflector.TypeInfoOf(x).Name
}
