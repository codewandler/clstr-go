package assert

import (
	"fmt"
)

type Func func() error
type CondFunc func() bool

type Cond interface {
	String() string
	Eval() bool
	Check() error
}

type cond struct {
	name  string
	cond  CondFunc
	check func() error
}

func (c *cond) Check() error   { return c.check() }
func (c *cond) String() string { return c.name }
func (c *cond) Eval() bool     { return c.cond() }

func newCond(name string, condFn CondFunc) *cond {
	return &cond{name: name, cond: condFn, check: func() error {
		if !condFn() {
			return fmt.Errorf("assertion failed: %s", name)
		}
		return nil
	}}
}

func Not(c Cond) Cond {
	return newCond(fmt.Sprintf("[not](%s)", c.String()), func() bool { return !c.Eval() })
}
func True(v bool, name string) Cond  { return newCond(name, func() bool { return v }) }
func False(v bool, name string) Cond { return newCond(name, func() bool { return !v }) }

func All(cs ...Cond) Cond {
	all := newCond("all", func() bool {
		for _, c := range cs {
			if !c.Eval() {
				return false
			}
		}
		return true
	})

	all.check = func() error {
		for _, c := range cs {
			if err := c.Check(); err != nil {
				return err
			}
		}
		return nil
	}

	return all
}

func Assert(cond ...Cond) Func {
	return All(cond...).Check
}
