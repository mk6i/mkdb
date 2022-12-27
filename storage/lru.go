package storage

import "container/list"

type LRUCache struct {
	cache    map[any]*list.Element
	list     *list.List
	maxNodes int
}

type cacheEntry struct {
	key any
	val any
}

func NewLRU(maxNodes int) *LRUCache {
	return &LRUCache{
		cache:    make(map[any]*list.Element),
		list:     list.New(),
		maxNodes: maxNodes,
	}
}

func (lru *LRUCache) set(key any, val any) {
	entry, found := lru.cache[key]
	if found {
		entry.Value.(*cacheEntry).val = val
		lru.list.MoveToFront(entry)
		return
	}

	elem := lru.list.PushFront(&cacheEntry{
		key: key,
		val: val,
	})
	lru.cache[key] = elem
	if len(lru.cache) > lru.maxNodes {
		// remove last entry
		last := lru.list.Back()
		lru.list.Remove(last)
		delete(lru.cache, last.Value.(*cacheEntry).key)
	}
}

func (lru *LRUCache) get(key any) (any, bool) {
	entry, found := lru.cache[key]
	if found {
		lru.list.MoveToFront(entry)
		return entry.Value.(*cacheEntry).val, true
	}
	return nil, false
}
