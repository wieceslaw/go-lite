package pagealloc

type PageId uint64

const (
	PageSize uint64 = 4096
)

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
