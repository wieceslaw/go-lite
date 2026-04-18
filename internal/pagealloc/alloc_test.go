package pagealloc

import (
	"errors"
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

func (f *fakeMmap) Expand(pages uint64) (PageInterval, error) {
	if pages == 0 {
		return PageInterval{}, nil
	}
	cur := uint64(len(f.pages))
	first := PageId(cur)
	if err := f.Resize(cur + pages); err != nil {
		return PageInterval{}, err
	}
	return PageInterval{First: first, Last: first + PageId(pages)}, nil
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

func (f *fakeMmap) LoadPages(iv PageInterval) ([]PageHandle, error) {
	n := iv.Length()
	if n == 0 {
		return nil, nil
	}
	out := make([]PageHandle, 0, n)
	for id := iv.First; id < iv.Last; id++ {
		ph, err := f.LoadPage(id)
		if err != nil {
			for _, h := range out {
				h.Close()
			}
			return nil, err
		}
		out = append(out, ph)
	}
	return out, nil
}

var _ MmapFile = (*fakeMmap)(nil)

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

func TestAllocate_fromFreelist(t *testing.T) {
	t.Parallel()
	a := seedAllocator(t, 10)
	// First freed page becomes the trunk; remaining span [3,5) is recorded on that trunk.
	if err := a.Free(PageInterval{First: 2, Last: 5}); err != nil {
		t.Fatal(err)
	}
	handles, err := a.Allocate(2)
	if err != nil {
		t.Fatal(err)
	}
	if len(handles) != 2 {
		t.Fatalf("len=%d", len(handles))
	}
	if handles[0].Id() != 3 || handles[1].Id() != 4 {
		t.Fatalf("ids %v %v", handles[0].Id(), handles[1].Id())
	}
	if a.header.data.FirstTrunkPage != 2 {
		t.Fatalf("FirstTrunkPage=%d", a.header.data.FirstTrunkPage)
	}
	td, err := decodeTrunkPage(a.mmap.(*fakeMmap).pages[2])
	if err != nil {
		t.Fatal(err)
	}
	if len(td.Intervals) != 0 {
		t.Fatalf("trunk should be empty after full consume, got %+v", td.Intervals)
	}
}

func TestAllocate_expandFile(t *testing.T) {
	t.Parallel()
	a := seedAllocator(t, 3)
	handles, err := a.Allocate(2)
	if err != nil {
		t.Fatal(err)
	}
	if len(handles) != 2 {
		t.Fatalf("len=%d", len(handles))
	}
	if a.header.data.Pages != 5 {
		t.Fatalf("header Pages=%d, want 5", a.header.data.Pages)
	}
	fm := a.mmap.(*fakeMmap)
	if uint64(len(fm.pages)) != 5 {
		t.Fatalf("file pages=%d", len(fm.pages))
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

func TestAllocate_zeroPages(t *testing.T) {
	t.Parallel()
	a := seedAllocator(t, 5)
	handles, err := a.Allocate(0)
	if err != nil {
		t.Fatal(err)
	}
	if handles != nil {
		t.Fatalf("Allocate(0) = %v, want nil", handles)
	}
}

func TestFree_emptyInterval_noop(t *testing.T) {
	t.Parallel()
	a := seedAllocator(t, 5)
	before := a.header.data.FirstTrunkPage
	if err := a.Free(EmptyInterval); err != nil {
		t.Fatal(err)
	}
	if a.header.data.FirstTrunkPage != before {
		t.Fatalf("empty Free changed FirstTrunkPage: %v -> %v", before, a.header.data.FirstTrunkPage)
	}
}

func TestFree_invalidSpan_errors(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		iv   PageInterval
	}{
		{"first_zero", PageInterval{First: 0, Last: 1}},
		{"inverted", PageInterval{First: 5, Last: 3}},
		{"beyond_file", PageInterval{First: 1, Last: 11}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			a := seedAllocator(t, 10)
			err := a.Free(tc.iv)
			if err == nil {
				t.Fatal("expected error")
			}
			if !errors.Is(err, ErrInvalidPageSpan) {
				t.Fatalf("got %v, want ErrInvalidPageSpan", err)
			}
		})
	}
}

func TestAllocate_partialFromFreelistLeavesRemainder(t *testing.T) {
	t.Parallel()
	a := seedAllocator(t, 12)
	if err := a.Free(PageInterval{First: 2, Last: 9}); err != nil {
		t.Fatal(err)
	}
	handles, err := a.Allocate(3)
	if err != nil {
		t.Fatal(err)
	}
	if len(handles) != 3 {
		t.Fatalf("len=%d", len(handles))
	}
	if handles[0].Id() != 3 || handles[1].Id() != 4 || handles[2].Id() != 5 {
		t.Fatalf("ids: %v %v %v", handles[0].Id(), handles[1].Id(), handles[2].Id())
	}
	td, err := decodeTrunkPage(a.mmap.(*fakeMmap).pages[2])
	if err != nil {
		t.Fatal(err)
	}
	want := []PageInterval{{First: 6, Last: 9}}
	if !slices.Equal(td.Intervals, want) {
		t.Fatalf("intervals = %+v, want %+v", td.Intervals, want)
	}
}

func TestAllocate_expandAfterFreelistExhausted(t *testing.T) {
	t.Parallel()
	a := seedAllocator(t, 6)
	if err := a.Free(PageInterval{First: 2, Last: 5}); err != nil {
		t.Fatal(err)
	}
	// Consume [3,5) from freelist.
	h1, err := a.Allocate(2)
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range h1 {
		h.Close()
	}
	// Freelist empty; must grow file.
	h2, err := a.Allocate(4)
	if err != nil {
		t.Fatal(err)
	}
	if len(h2) != 4 {
		t.Fatalf("len=%d", len(h2))
	}
	if a.header.data.Pages != 10 {
		t.Fatalf("header Pages=%d, want 10", a.header.data.Pages)
	}
}

func TestAllocate_secondTrunkWhenFirstCannotSatisfy(t *testing.T) {
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
	if err := a.Free(PageInterval{First: lastFreeLo, Last: lastFreeLo + 2}); err != nil {
		t.Fatal(err)
	}
	// Extend the last trunk with a 4-page span merged into the same trunk page.
	if err := a.Free(PageInterval{First: lastFreeLo + 2, Last: lastFreeLo + 6}); err != nil {
		t.Fatal(err)
	}

	fm := a.mmap.(*fakeMmap)
	handles, err := a.Allocate(3)
	if err != nil {
		t.Fatal(err)
	}
	if len(handles) != 3 {
		t.Fatalf("len=%d", len(handles))
	}
	// After Free(lastFreeLo, lastFreeLo+2) and Free(lastFreeLo+2, lastFreeLo+6), the last
	// trunk holds [lastFreeLo+1, lastFreeLo+6); first trunk has only 1-page fragments.
	wantStart := lastFreeLo + 1
	if handles[0].Id() != wantStart || handles[1].Id() != wantStart+1 || handles[2].Id() != wantStart+2 {
		t.Fatalf("got ids starting %v, want %d,%d,%d", handles[0].Id(), wantStart, wantStart+1, wantStart+2)
	}

	lastTd, err := decodeTrunkPage(fm.pages[lastFreeLo])
	if err != nil {
		t.Fatal(err)
	}
	// Remainder after taking 3 pages from [lastFreeLo+1, lastFreeLo+6).
	wantRem := []PageInterval{{First: lastFreeLo + 4, Last: lastFreeLo + 6}}
	if !slices.Equal(lastTd.Intervals, wantRem) {
		t.Fatalf("last trunk intervals = %+v, want %+v", lastTd.Intervals, wantRem)
	}
}

func TestAllocate_free_allocate_roundTrip(t *testing.T) {
	t.Parallel()
	a := seedAllocator(t, 8)
	// Trunk at 2, freelist [3,7) — four pages.
	if err := a.Free(PageInterval{First: 2, Last: 7}); err != nil {
		t.Fatal(err)
	}
	h1, err := a.Allocate(4)
	if err != nil {
		t.Fatal(err)
	}
	ids1 := make([]PageId, len(h1))
	for i, h := range h1 {
		ids1[i] = h.Id()
		h.Close()
	}
	if err := a.Free(PageInterval{First: ids1[0], Last: ids1[len(ids1)-1] + 1}); err != nil {
		t.Fatal(err)
	}
	h2, err := a.Allocate(4)
	if err != nil {
		t.Fatal(err)
	}
	if len(h2) != 4 {
		t.Fatalf("len=%d", len(h2))
	}
	for i := range h2 {
		if h2[i].Id() != ids1[i] {
			t.Fatalf("re-alloc id[%d]=%v, want %v", i, h2[i].Id(), ids1[i])
		}
		h2[i].Close()
	}
}
