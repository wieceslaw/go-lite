package pagealloc

import (
	"os"
	"slices"
	"testing"
)

// fakeMmap is an in-memory MmapFile for tests: growable pages, short reads zero-filled (like PageFile).
type fakeMmap struct {
	pages [][]byte
}

func newFakeMmap(initialPages uint64) *fakeMmap {
	if initialPages < 1 {
		initialPages = 1
	}
	f := &fakeMmap{pages: make([][]byte, initialPages)}
	for i := range f.pages {
		f.pages[i] = make([]byte, PageSize)
	}
	return f
}

func (f *fakeMmap) Resize(pages uint64) error {
	if pages < uint64(len(f.pages)) {
		f.pages = f.pages[:pages]
		return nil
	}
	for uint64(len(f.pages)) < pages {
		f.pages = append(f.pages, make([]byte, PageSize))
	}
	return nil
}

func (f *fakeMmap) FilePages() (uint64, error) {
	return uint64(len(f.pages)), nil
}

type fakePageHandle struct {
	fake *fakeMmap
	id   PageId
	data []byte
}

func (h *fakePageHandle) Id() PageId { return h.id }

func (h *fakePageHandle) Read() []byte { return h.data }

func (h *fakePageHandle) Write(data []byte) {
	n := copy(h.data, data)
	for i := n; i < len(h.data); i++ {
		h.data[i] = 0
	}
}

func (h *fakePageHandle) Flush() error {
	copy(h.fake.pages[h.id], h.data)
	return nil
}

func (h *fakePageHandle) Close() {}

func (f *fakeMmap) LoadPage(id PageId) (PageHandle, error) {
	if uint64(id) >= uint64(len(f.pages)) {
		return nil, os.ErrNotExist
	}
	buf := slices.Clone(f.pages[id])
	return &fakePageHandle{fake: f, id: id, data: buf}, nil
}

// seedAllocator opens an allocator on a fake file, grows to nPages, and sets header.Pages (synced to page 0).
func seedAllocator(t *testing.T, nPages uint64) *pageAllocatorImpl {
	t.Helper()
	f := newFakeMmap(1)
	alloc, err := NewPageAllocator(f)
	if err != nil {
		t.Fatal(err)
	}
	impl := alloc
	if err := f.Resize(nPages); err != nil {
		t.Fatal(err)
	}
	impl.header.data.Pages = nPages
	if err := impl.header.sync(); err != nil {
		t.Fatal(err)
	}
	return impl
}

func TestNewPageAllocator_initsEmptyHeader(t *testing.T) {
	t.Parallel()
	f := newFakeMmap(1)
	alloc, err := NewPageAllocator(f)
	if err != nil {
		t.Fatal(err)
	}
	_ = alloc
	h, err := decodeHeader(f.pages[0])
	if err != nil {
		t.Fatal(err)
	}
	if h.Magic != headerMagic || h.Version != headerVersion {
		t.Fatalf("header %+v", h)
	}
	if h.Pages != 1 {
		t.Fatalf("Pages = %d, want 1", h.Pages)
	}
	if h.FirstTrunkPage != EmptyTrunkID || h.LastTrunkPage != EmptyTrunkID {
		t.Fatalf("trunk ids should be empty, got %+v", h)
	}
}

func TestFree_firstTrunk(t *testing.T) {
	t.Parallel()
	a := seedAllocator(t, 5)
	err := a.Free(PageInterval{First: 1, Last: 3})
	if err != nil {
		t.Fatal(err)
	}
	if a.header.data.FirstTrunkPage != 1 || a.header.data.LastTrunkPage != 1 {
		t.Fatalf("header trunks: first=%d last=%d", a.header.data.FirstTrunkPage, a.header.data.LastTrunkPage)
	}
	td, err := decodeTrunkPage(a.mmap.(*fakeMmap).pages[1])
	if err != nil {
		t.Fatal(err)
	}
	if td.NextId != EmptyTrunkID {
		t.Fatalf("NextId = %v", td.NextId)
	}
	want := []PageInterval{{First: 2, Last: 3}}
	if !slices.Equal(td.Intervals, want) {
		t.Fatalf("intervals = %+v, want %+v", td.Intervals, want)
	}
}

func TestFree_mergeIntoLastTrunk(t *testing.T) {
	t.Parallel()
	a := seedAllocator(t, 10)
	if err := a.Free(PageInterval{First: 1, Last: 3}); err != nil {
		t.Fatal(err)
	}
	if err := a.Free(PageInterval{First: 3, Last: 5}); err != nil {
		t.Fatal(err)
	}
	td, err := decodeTrunkPage(a.mmap.(*fakeMmap).pages[1])
	if err != nil {
		t.Fatal(err)
	}
	want := []PageInterval{{First: 2, Last: 5}}
	if !slices.Equal(td.Intervals, want) {
		t.Fatalf("intervals = %+v, want %+v", td.Intervals, want)
	}
}

func TestFree_newTrunkWhenFull(t *testing.T) {
	t.Parallel()
	m := trunkMaxIntervalsPerPage
	nPages := uint64(2*m + 8)
	a := seedAllocator(t, nPages)

	if err := a.Free(PageInterval{First: 1, Last: 3}); err != nil {
		t.Fatal(err)
	}
	for j := uint64(0); j < m-1; j++ {
		lo := PageId(4 + 2*j)
		if err := a.Free(PageInterval{First: lo, Last: lo + 1}); err != nil {
			t.Fatalf("j=%d: %v", j, err)
		}
	}

	lastFreeLo := PageId(2*m + 2)
	if err := a.Free(PageInterval{First: lastFreeLo, Last: lastFreeLo + 1}); err != nil {
		t.Fatal(err)
	}

	fm := a.mmap.(*fakeMmap)
	prev, err := decodeTrunkPage(fm.pages[1])
	if err != nil {
		t.Fatal(err)
	}
	if prev.NextId != lastFreeLo {
		t.Fatalf("first trunk NextId = %v, want %d", prev.NextId, lastFreeLo)
	}
	last, err := decodeTrunkPage(fm.pages[lastFreeLo])
	if err != nil {
		t.Fatal(err)
	}
	if last.NextId != EmptyTrunkID {
		t.Fatalf("last trunk NextId = %v", last.NextId)
	}
	if a.header.data.LastTrunkPage != lastFreeLo {
		t.Fatalf("header LastTrunkPage = %d, want %d", a.header.data.LastTrunkPage, lastFreeLo)
	}
}
