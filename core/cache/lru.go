package cache

import (
	"container/list"
	"time"
)

type LRUOpts struct {
	Size int
}

type entry struct {
	key       string
	val       any
	expiresAt time.Time
}

func (e *entry) expired(now time.Time) bool {
	return !e.expiresAt.IsZero() && now.After(e.expiresAt)
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

type delReq struct {
	key string
}

type LRU struct {
	getCh  chan getReq
	putCh  chan putReq
	delCh  chan delReq
	doneCh chan struct{}
}

func (L *LRU) Get(key string) (any, bool) {
	resp := make(chan getResp)
	select {
	case L.getCh <- getReq{key: key, resp: resp}:
		r := <-resp
		return r.val, r.ok
	case <-L.doneCh:
		return nil, false
	}
}

func (L *LRU) Put(key string, val any, opts ...PutOption) {
	select {
	case L.putCh <- putReq{key: key, val: val, opts: opts}:
	case <-L.doneCh:
	}
}

func (L *LRU) Delete(key string) {
	select {
	case L.delCh <- delReq{key: key}:
	case <-L.doneCh:
	}
}

func (L *LRU) Close() {
	close(L.doneCh)
}

func NewLRU(opts LRUOpts) *LRU {
	if opts.Size <= 0 {
		opts.Size = 128
	}

	l := &LRU{
		getCh:  make(chan getReq),
		putCh:  make(chan putReq),
		delCh:  make(chan delReq),
		doneCh: make(chan struct{}),
	}

	go l.run(opts.Size)

	return l
}

func (L *LRU) run(size int) {
	ll := list.New()
	cache := make(map[string]*list.Element)

	for {
		select {
		case <-L.doneCh:
			return
		case req := <-L.getCh:
			if ele, ok := cache[req.key]; ok {
				e := ele.Value.(*entry)
				if e.expired(time.Now()) {
					ll.Remove(ele)
					delete(cache, req.key)
					req.resp <- getResp{ok: false}
				} else {
					ll.MoveToFront(ele)
					req.resp <- getResp{val: e.val, ok: true}
				}
			} else {
				req.resp <- getResp{ok: false}
			}
		case req := <-L.putCh:
			var po PutOptions
			for _, opt := range req.opts {
				opt(&po)
			}

			var expiresAt time.Time
			if po.TTL > 0 {
				expiresAt = time.Now().Add(po.TTL)
			}

			if ele, ok := cache[req.key]; ok {
				ll.MoveToFront(ele)
				e := ele.Value.(*entry)
				e.val = req.val
				e.expiresAt = expiresAt
			} else {
				ele := ll.PushFront(&entry{key: req.key, val: req.val, expiresAt: expiresAt})
				cache[req.key] = ele
				if ll.Len() > size {
					last := ll.Back()
					if last != nil {
						ll.Remove(last)
						delete(cache, last.Value.(*entry).key)
					}
				}
			}
		case req := <-L.delCh:
			if ele, ok := cache[req.key]; ok {
				ll.Remove(ele)
				delete(cache, req.key)
			}
		}
	}
}

var _ Cache = (*LRU)(nil)
