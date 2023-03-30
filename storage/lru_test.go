package storage

import (
	"testing"
)

func TestLRUGet_Found(t *testing.T) {

	lru := NewLRU(3)

	lru.set("A", &btreeNode{isLeaf: true, fileOffset: 1})
	lru.set("B", &btreeNode{isLeaf: true, fileOffset: 2})
	lru.set("C", &btreeNode{isLeaf: true, fileOffset: 3})

	key := "A"
	expect := &btreeNode{isLeaf: true, fileOffset: 1}

	val, found := lru.get(key)
	if !found {
		t.Fatalf("did not find entry for key %v", key)
	}

	if expect.getFileOffset() != val.getFileOffset() {
		t.Fatalf("expected value %v, got %v", expect, val)
	}
}

func TestLRUGet_NotFound(t *testing.T) {

	lru := NewLRU(3)

	lru.set("A", &btreeNode{isLeaf: true, fileOffset: 1})
	lru.set("B", &btreeNode{isLeaf: true, fileOffset: 2})
	lru.set("C", &btreeNode{isLeaf: true, fileOffset: 3})

	key := "D"
	val, found := lru.get(key)
	if found {
		t.Fatalf("unexpectedly found entry for key %v, got %v", key, val)
	}
}

func TestLRUState(t *testing.T) {

	tests := []struct {
		name        string
		expectCount int
		maxEntries  int
		setState    func(*LRUCache)
		expectFront any
		expectBack  any
	}{
		{
			name:        "adding new element to full cache should evict oldest element",
			maxEntries:  5,
			expectCount: 5,
			setState: func(lru *LRUCache) {
				lru.set("A", &btreeNode{isLeaf: true, fileOffset: 1})
				lru.set("B", &btreeNode{isLeaf: true, fileOffset: 2})
				lru.set("C", &btreeNode{isLeaf: true, fileOffset: 3})
				lru.set("D", &btreeNode{isLeaf: true, fileOffset: 4})
				lru.set("E", &btreeNode{isLeaf: true, fileOffset: 5})
				lru.set("F", &btreeNode{isLeaf: true, fileOffset: 6}) // this should evict element A
			},
			expectFront: "F",
			expectBack:  "B",
		},
		{
			name:        "updating existing element should move it to the head of the list",
			expectCount: 3,
			maxEntries:  5,
			setState: func(lru *LRUCache) {
				lru.set("A", &btreeNode{isLeaf: true, fileOffset: 1})
				lru.set("B", &btreeNode{isLeaf: true, fileOffset: 2})
				lru.set("C", &btreeNode{isLeaf: true, fileOffset: 3})
				lru.set("A", &btreeNode{isLeaf: true, fileOffset: 1}) // this should be moved to beginning of list
			},
			expectFront: "A",
			expectBack:  "B",
		},
		{
			name:        "retrieving existing element should move it to the head of the list",
			expectCount: 3,
			maxEntries:  5,
			setState: func(lru *LRUCache) {
				lru.set("A", &btreeNode{isLeaf: true, fileOffset: 1})
				lru.set("B", &btreeNode{isLeaf: true, fileOffset: 2})
				lru.set("C", &btreeNode{isLeaf: true, fileOffset: 3})
				lru.get("A") // this should be moved to beginning of list
			},
			expectFront: "A",
			expectBack:  "B",
		},
		{
			name:        "retrieving non-existing element should not affect list",
			expectCount: 3,
			maxEntries:  5,
			setState: func(lru *LRUCache) {
				lru.set("A", &btreeNode{isLeaf: true, fileOffset: 1})
				lru.set("B", &btreeNode{isLeaf: true, fileOffset: 2})
				lru.set("C", &btreeNode{isLeaf: true, fileOffset: 3})
				lru.get("X") // this element should not exist
			},
			expectFront: "C",
			expectBack:  "A",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lru := NewLRU(test.maxEntries)
			test.setState(lru)
			if test.expectCount != len(lru.cache) {
				t.Fatalf("cache element count is unexpected. expected: %v actual: %v \ncache:\n\t%v", test.expectCount, len(lru.cache), lru.cache)
			}

			if lru.list.Front().Value.(*cacheEntry).key != test.expectFront {
				t.Fatalf("head of list is expected to be %v, got %v", test.expectFront, lru.list.Front().Value.(*cacheEntry).key)
			}

			if lru.list.Back().Value.(*cacheEntry).key != test.expectBack {
				t.Fatalf("back of list is expected to be %v, got %v", test.expectBack, lru.list.Back().Value.(*cacheEntry).key)
			}
		})
	}
}

func TestLRUEviction(t *testing.T) {

	tests := []struct {
		name        string
		expectCount int
		maxEntries  int
		setState    func(*LRUCache)
		expectKeys  []string
	}{
		{
			name:        "adding new element to full cache should evict oldest element",
			maxEntries:  3,
			expectCount: 3,
			setState: func(lru *LRUCache) {
				lru.set("A", &btreeNode{isLeaf: true, fileOffset: 1})
				lru.set("B", &btreeNode{isLeaf: true, fileOffset: 2})
				lru.set("C", &btreeNode{isLeaf: true, fileOffset: 3})
				lru.set("D", &btreeNode{isLeaf: true, fileOffset: 4}) // this should evict element A
			},
			expectKeys: []string{"D", "C", "B"},
		},
		{
			name:        "adding new element to full cache should evict 2nd-oldest element",
			maxEntries:  3,
			expectCount: 3,
			setState: func(lru *LRUCache) {
				lru.set("A", &btreeNode{isLeaf: true, fileOffset: 1, dirty: true})
				lru.set("B", &btreeNode{isLeaf: true, fileOffset: 2})
				lru.set("C", &btreeNode{isLeaf: true, fileOffset: 3})
				lru.set("D", &btreeNode{isLeaf: true, fileOffset: 4}) // this should evict element B
			},
			expectKeys: []string{"D", "C", "A"},
		},
		{
			name:        "adding new element to full cache should evict 3rd-oldest element",
			maxEntries:  3,
			expectCount: 3,
			setState: func(lru *LRUCache) {
				lru.set("A", &btreeNode{isLeaf: true, fileOffset: 1, dirty: true})
				lru.set("B", &btreeNode{isLeaf: true, fileOffset: 2, dirty: true})
				lru.set("C", &btreeNode{isLeaf: true, fileOffset: 3})
				lru.set("D", &btreeNode{isLeaf: true, fileOffset: 4}) // this should evict element C
			},
			expectKeys: []string{"D", "B", "A"},
		},
		{
			name:        "adding new element to full cache should fail",
			maxEntries:  3,
			expectCount: 3,
			setState: func(lru *LRUCache) {
				lru.set("A", &btreeNode{isLeaf: true, fileOffset: 1, dirty: true})
				lru.set("B", &btreeNode{isLeaf: true, fileOffset: 2, dirty: true})
				lru.set("C", &btreeNode{isLeaf: true, fileOffset: 3, dirty: true})
				if lru.set("D", &btreeNode{isLeaf: true, fileOffset: 4}) {
					t.Fatalf("expected set on cache full of dirty pages to fail, but it succeeded")
				}
			},
			expectKeys: []string{"C", "B", "A"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lru := NewLRU(test.maxEntries)
			test.setState(lru)
			if test.expectCount != len(lru.cache) {
				t.Fatalf("cache element count is unexpected. expected: %v actual: %v \ncache:\n\t%v", test.expectCount, len(lru.cache), lru.cache)
			}

			cur := lru.list.Front()
			for i, expectK := range test.expectKeys {
				actualK := cur.Value.(*cacheEntry).key
				if expectK != actualK {
					t.Fatalf("cache key is not the same at position %d. expected: %v got: %v", i, expectK, actualK)
				}
				cur = cur.Next()
			}
		})
	}
}
