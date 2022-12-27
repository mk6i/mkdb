package storage

import (
	"testing"
)

func TestLRUGet_Found(t *testing.T) {

	lru := NewLRU(3)

	lru.set("A", "red")
	lru.set("B", "blue")
	lru.set("C", "green")

	key := "A"
	var expect any
	expect = "red"

	val, found := lru.get(key)
	if !found {
		t.Fatalf("did not find entry for key %v", key)
	}

	if expect != val {
		t.Fatalf("expected value %v, got %v", expect, val)
	}
}

func TestLRUGet_NotFound(t *testing.T) {

	lru := NewLRU(3)

	lru.set("A", "red")
	lru.set("B", "blue")
	lru.set("C", "green")

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
				lru.set("A", "red")
				lru.set("B", "blue")
				lru.set("C", "green")
				lru.set("D", "orange")
				lru.set("E", "yellow")
				lru.set("F", "brown") // this should evict element A
			},
			expectFront: "F",
			expectBack:  "B",
		},
		{
			name:        "updating existing element should move it to the head of the list",
			expectCount: 3,
			maxEntries:  5,
			setState: func(lru *LRUCache) {
				lru.set("A", "red")
				lru.set("B", "blue")
				lru.set("C", "green")
				lru.set("A", "red") // this should be moved to beginning of list
			},
			expectFront: "A",
			expectBack:  "B",
		},
		{
			name:        "retrieving existing element should move it to the head of the list",
			expectCount: 3,
			maxEntries:  5,
			setState: func(lru *LRUCache) {
				lru.set("A", "red")
				lru.set("B", "blue")
				lru.set("C", "green")
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
				lru.set("A", "red")
				lru.set("B", "blue")
				lru.set("C", "green")
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
