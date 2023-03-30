package storage

import "container/list"

type LRUCache struct {
	cache    map[any]*list.Element
	list     *list.List
	maxNodes int
}

type cacheEntry struct {
	key any
	val *btreeNode
}

func NewLRU(maxNodes int) *LRUCache {
	return &LRUCache{
		cache:    make(map[any]*list.Element),
		list:     list.New(),
		maxNodes: maxNodes,
	}
}

func (lru *LRUCache) set(key any, val *btreeNode) bool {
	entry, found := lru.cache[key]
	if found {
		entry.Value.(*cacheEntry).val = val
		lru.list.MoveToFront(entry)
		return true
	}

	if len(lru.cache) == lru.maxNodes {
		// remove oldest, non-dirty entry
		cur := lru.list.Back()
		for {
			if cur == nil {
				return false
			} else if !cur.Value.(*cacheEntry).val.isDirty() {
				break
			} else {
				cur = cur.Prev()
			}
		}

		lru.list.Remove(cur)
		delete(lru.cache, cur.Value.(*cacheEntry).key)
	}

	elem := lru.list.PushFront(&cacheEntry{
		key: key,
		val: val,
	})
	lru.cache[key] = elem
	return true
}

func (lru *LRUCache) get(key any) (*btreeNode, bool) {
	entry, found := lru.cache[key]
	if found {
		lru.list.MoveToFront(entry)
		return entry.Value.(*cacheEntry).val, true
	}
	return nil, false
}
