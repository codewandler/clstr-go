package cache

type Nop struct{}

func (n *Nop) Get(key string) (any, bool) {
	return nil, false
}

func (n *Nop) Put(key string, val any, opts ...PutOption) {
}

func NewNop() *Nop {
	return &Nop{}
}

var _ Cache = (*Nop)(nil)
