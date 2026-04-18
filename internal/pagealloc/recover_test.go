package pagealloc

import "testing"

func TestRecover_corruptTrunkRewritesEmpty(t *testing.T) {
	t.Parallel()
	a := seedAllocator(t, 5)
	if err := a.Free(PageInterval{First: 1, Last: 3}); err != nil {
		t.Fatal(err)
	}
	fm := a.mmap.(*fakeMmap)
	fm.pages[1][0] ^= 0xff

	if err := a.recover(); err != nil {
		t.Fatal(err)
	}
	td, err := decodeTrunkPage(fm.pages[1])
	if err != nil {
		t.Fatal(err)
	}
	if td.NextId != EmptyTrunkID || len(td.Intervals) != 0 {
		t.Fatalf("expected empty trunk, got %+v", td)
	}
}

func TestRecover_repairsLastTrunkPage(t *testing.T) {
	t.Parallel()
	a := seedAllocator(t, 10)
	fm := a.mmap.(*fakeMmap)
	td1 := trunk{NextId: 2, Intervals: []PageInterval{{10, 11}}}
	td2 := trunk{NextId: EmptyTrunkID, Intervals: []PageInterval{{20, 21}}}
	if err := encodeTrunkPage(td1, fm.pages[1]); err != nil {
		t.Fatal(err)
	}
	if err := encodeTrunkPage(td2, fm.pages[2]); err != nil {
		t.Fatal(err)
	}
	a.header.data.FirstTrunkPage = 1
	a.header.data.LastTrunkPage = 1
	if err := a.header.sync(); err != nil {
		t.Fatal(err)
	}

	if err := a.recover(); err != nil {
		t.Fatal(err)
	}
	if a.header.data.LastTrunkPage != 2 {
		t.Fatalf("LastTrunkPage = %d, want 2", a.header.data.LastTrunkPage)
	}
}

func TestRecover_syncBackingPagesToHeader_resizes(t *testing.T) {
	t.Parallel()
	a := seedAllocator(t, 10)
	fm := a.mmap.(*fakeMmap)
	if len(fm.pages) != 10 {
		t.Fatalf("len = %d", len(fm.pages))
	}
	fm.pages = fm.pages[:3]

	if err := a.recover(); err != nil {
		t.Fatal(err)
	}
	n, err := fm.FilePages()
	if err != nil {
		t.Fatal(err)
	}
	if n != 10 || len(fm.pages) != 10 {
		t.Fatalf("FilePages=%d len=%d, want 10", n, len(fm.pages))
	}
}
