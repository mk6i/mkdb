package btree

type BTree struct {
	store
}

func (b *BTree) insert(p *page) (uint32, error) {
	return b.store.append(p)
}

func (b *BTree) find(key uint32) (*page, error) {
	return b.store.fetch(uint16(key))
}

// binary search looks for needle in haystack. if found is true, offset is the
// position of val in the slice. if found is false, offset is val's insertion
// point (the index of the first element greater than val).
func binarySearch(val uint16, elems []uint16) (offset int, found bool) {
	low := 0
	high := len(elems) - 1

	for low <= high {
		mid := low + (high-low)/2

		switch {
		case elems[mid] == val:
			return mid, true
		case elems[mid] < val:
			low = mid + 1
		default:
			high = mid - 1
		}
	}

	return low, false
}
