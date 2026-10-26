// Package cache is a small LRU cache with per-entry expiry.
package cache

import (
	"container/list"
	"sync"
	"time"
)

type entry struct {
	key     string
	val     []byte
	expires time.Time
}

type Cache struct {
	mu    sync.Mutex
	max   int
	ll    *list.List // front = most recently used
	items map[string]*list.Element
	now   func() time.Time
}

// New returns a cache holding at most max entries. A max of zero or less disables caching.
func New(max int) *Cache {
	return &Cache{max: max, ll: list.New(), items: make(map[string]*list.Element), now: time.Now}
}

func (c *Cache) Get(key string) ([]byte, bool) {
	if c.max <= 0 {
		return nil, false
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	el, ok := c.items[key]
	if !ok {
		return nil, false
	}

	e := el.Value.(*entry)
	if !c.now().Before(e.expires) {
		c.remove(el)
		return nil, false
	}

	c.ll.MoveToFront(el)
	return e.val, true
}

func (c *Cache) Set(key string, val []byte, ttl time.Duration) {
	if c.max <= 0 || ttl <= 0 {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	expires := c.now().Add(ttl)
	if el, ok := c.items[key]; ok {
		e := el.Value.(*entry)
		e.val, e.expires = val, expires
		c.ll.MoveToFront(el)
		return
	}

	c.items[key] = c.ll.PushFront(&entry{key: key, val: val, expires: expires})
	for c.ll.Len() > c.max {
		c.remove(c.ll.Back())
	}
}

func (c *Cache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ll.Len()
}

func (c *Cache) remove(el *list.Element) {
	c.ll.Remove(el)
	delete(c.items, el.Value.(*entry).key)
}
