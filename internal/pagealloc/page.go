package pagealloc

type PageId uint64

// PageSize is the on-disk page granularity: the file length is a multiple of
// PageSize, and Header.PageSizeConst must match this value.
const (
	PageSize uint64 = 4096
)

var EmptyInterval = PageInterval{}

// PageInterval is a half-open page range [First, Last): First is inclusive, Last is exclusive.
type PageInterval struct {
	First PageId
	Last  PageId
}

func (iv PageInterval) Length() uint64 {
	if iv.Last <= iv.First {
		return 0
	}
	return uint64(iv.Last - iv.First)
}

func (iv PageInterval) IsEmpty() bool {
	return iv.First == iv.Last
}

func (iv PageInterval) SplitFirst() (PageId, PageInterval) {
	return iv.First, PageInterval{First: iv.First + 1, Last: iv.Last}
}

// SplitBySize returns the first n pages as head and the remainder as tail (both half-open).
// ok is false if n == 0 or n > Length().
func (iv PageInterval) SplitBySize(n uint64) (head, tail PageInterval, ok bool) {
	if n == 0 || iv.Length() < n {
		return PageInterval{}, PageInterval{}, false
	}
	split := iv.First + PageId(n)
	return PageInterval{First: iv.First, Last: split}, PageInterval{First: split, Last: iv.Last}, true
}
