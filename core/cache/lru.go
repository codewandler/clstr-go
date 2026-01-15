package cache

import (
	"container/list"
)

type LRUOpts struct {
	Size int
}

type entry struct {
	key string
	val any
}

type getReq struct {
	key  string
	resp chan getResp
}

type getResp struct {
	val any
	ok  bool
}

type putReq struct {
	key  string
	val  any
	opts []PutOption
}

type LRU struct {
	getCh chan getReq
	putCh chan putReq
}

func (L *LRU) Get(key string) (any, bool) {
	resp := make(chan getResp)
	L.getCh <- getReq{key: key, resp: resp}
	r := <-resp
	return r.val, r.ok
}

func (L *LRU) Put(key string, val any, opts ...PutOption) {
	L.putCh <- putReq{key: key, val: val, opts: opts}
}

func NewLRU(opts LRUOpts) *LRU {
	if opts.Size <= 0 {
		opts.Size = 128
	}

	l := &LRU{
		getCh: make(chan getReq),
		putCh: make(chan putReq),
	}

	go l.run(opts.Size)

	return l
}

func (L *LRU) run(size int) {
	ll := list.New()
	cache := make(map[string]*list.Element)

	for {
		select {
		case req := <-L.getCh:
			if ele, ok := cache[req.key]; ok {
				ll.MoveToFront(ele)
				req.resp <- getResp{val: ele.Value.(*entry).val, ok: true}
			} else {
				req.resp <- getResp{ok: false}
			}
		case req := <-L.putCh:
			if ele, ok := cache[req.key]; ok {
				ll.MoveToFront(ele)
				ele.Value.(*entry).val = req.val
			} else {
				ele := ll.PushFront(&entry{key: req.key, val: req.val})
				cache[req.key] = ele
				if ll.Len() > size {
					last := ll.Back()
					if last != nil {
						ll.Remove(last)
						delete(cache, last.Value.(*entry).key)
					}
				}
			}
		}
	}
}

var _ Cache = (*LRU)(nil)
