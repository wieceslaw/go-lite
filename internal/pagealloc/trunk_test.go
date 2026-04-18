package pagealloc

import (
	"errors"
	"fmt"
	"slices"
	"testing"
)

func TestMergeAdjacentSorted(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		in   []PageInterval
		want []PageInterval
	}{
		{"nil", nil, nil},
		{"empty", []PageInterval{}, []PageInterval{}},
		{"single", []PageInterval{{1, 3}}, []PageInterval{{1, 3}}},
		{"adjacent_half_open", []PageInterval{{0, 1}, {1, 2}}, []PageInterval{{0, 2}}},
		{"touching_ranges", []PageInterval{{0, 2}, {2, 4}}, []PageInterval{{0, 4}}},
		{"overlap", []PageInterval{{0, 5}, {2, 8}}, []PageInterval{{0, 8}}},
		{"disjoint", []PageInterval{{0, 1}, {2, 3}}, []PageInterval{{0, 1}, {2, 3}}},
		{"three_chain", []PageInterval{{0, 1}, {1, 2}, {2, 3}}, []PageInterval{{0, 3}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := mergeAdjacentSorted(slices.Clone(tc.in))
			if !slices.Equal(got, tc.want) {
				t.Fatalf("mergeAdjacentSorted(%v) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

func TestMergeItervals(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		ivs  []PageInterval
		add  PageInterval
		want []PageInterval
	}{
		{
			name: "nil_list_adds_one",
			ivs:  nil,
			add:  PageInterval{0, 1},
			want: []PageInterval{{0, 1}},
		},
		{
			name: "unsorted_merge",
			ivs:  []PageInterval{{10, 20}},
			add:  PageInterval{0, 5},
			want: []PageInterval{{0, 5}, {10, 20}},
		},
		{
			name: "merge_with_existing",
			ivs:  []PageInterval{{0, 2}},
			add:  PageInterval{2, 4},
			want: []PageInterval{{0, 4}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := mergeItervals(slices.Clone(tc.ivs), tc.add)
			if !slices.Equal(got, tc.want) {
				t.Fatalf("mergeItervals(%v, %v) = %v, want %v", tc.ivs, tc.add, got, tc.want)
			}
		})
	}
}

func TestTrunk_acquireInterval(t *testing.T) {
	t.Parallel()
	t.Run("exact_fit_removes_interval", func(t *testing.T) {
		t.Parallel()
		tr := trunk{NextId: 7, Intervals: []PageInterval{{10, 13}}}
		iv, got, ok := tr.acquireInterval(3)
		if !ok {
			t.Fatal("expected ok")
		}
		wantIv := PageInterval{First: 10, Last: 13}
		if iv != wantIv {
			t.Fatalf("acquired %v, want %v", iv, wantIv)
		}
		wantTr := trunk{NextId: 7, Intervals: nil}
		if got.NextId != wantTr.NextId || !slices.Equal(got.Intervals, wantTr.Intervals) {
			t.Fatalf("trunk %+v, want %+v", got, wantTr)
		}
	})

	t.Run("split_leaves_remainder", func(t *testing.T) {
		t.Parallel()
		tr := trunk{Intervals: []PageInterval{{10, 20}}}
		iv, got, ok := tr.acquireInterval(3)
		if !ok {
			t.Fatal("expected ok")
		}
		if iv != (PageInterval{First: 10, Last: 13}) {
			t.Fatalf("iv %v", iv)
		}
		want := []PageInterval{{13, 20}}
		if !slices.Equal(got.Intervals, want) {
			t.Fatalf("intervals %+v, want %+v", got.Intervals, want)
		}
	})

	t.Run("first_fit_in_list_order", func(t *testing.T) {
		t.Parallel()
		tr := trunk{Intervals: []PageInterval{{1, 2}, {5, 10}}}
		iv, got, ok := tr.acquireInterval(4)
		if !ok {
			t.Fatal("expected ok")
		}
		if iv.First != 5 || iv.Last != 9 {
			t.Fatalf("iv %v", iv)
		}
		if !slices.Equal(got.Intervals, []PageInterval{{1, 2}, {9, 10}}) {
			t.Fatalf("got %+v", got.Intervals)
		}
	})

	t.Run("too_small", func(t *testing.T) {
		t.Parallel()
		tr := trunk{Intervals: []PageInterval{{1, 2}}}
		_, _, ok := tr.acquireInterval(2)
		if ok {
			t.Fatal("expected not ok")
		}
	})
}

func TestMergeIntoTrunkIfFits(t *testing.T) {
	t.Parallel()
	t.Run("fits_simple", func(t *testing.T) {
		t.Parallel()
		td := trunk{NextId: 42, Intervals: []PageInterval{{1, 2}}}
		got, ok := mergeIntoTrunkIfFits(td, PageInterval{2, 4})
		if !ok {
			t.Fatal("expected ok")
		}
		want := trunk{NextId: 42, Intervals: []PageInterval{{1, 4}}}
		if got.NextId != want.NextId || !slices.Equal(got.Intervals, want.Intervals) {
			t.Fatalf("got %+v, want %+v", got, want)
		}
	})

	t.Run("does_not_fit_segment_cap", func(t *testing.T) {
		t.Parallel()
		ivs := make([]PageInterval, trunkMaxIntervalsPerPage)
		for i := range ivs {
			base := PageId(2 * i)
			ivs[i] = PageInterval{First: base, Last: base + 1}
		}
		td := trunk{NextId: 7, Intervals: ivs}
		add := PageInterval{First: PageId(2 * trunkMaxIntervalsPerPage), Last: PageId(2*trunkMaxIntervalsPerPage) + 1}
		_, ok := mergeIntoTrunkIfFits(td, add)
		if ok {
			t.Fatal("expected merge to exceed one trunk page")
		}
	})
}

func TestEncodeDecodeTrunkPage_roundTrip(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		td   trunk
	}{
		{"empty_next", trunk{NextId: EmptyTrunkID, Intervals: nil}},
		{"with_next", trunk{NextId: 99, Intervals: []PageInterval{{1, 3}, {10, 11}}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			buf := make([]byte, PageSize)
			if err := encodeTrunkPage(tc.td, buf); err != nil {
				t.Fatal(err)
			}
			got, err := decodeTrunkPage(buf)
			if err != nil {
				t.Fatal(err)
			}
			if got.NextId != tc.td.NextId || !slices.Equal(got.Intervals, tc.td.Intervals) {
				t.Fatalf("decode = %+v, want %+v", got, tc.td)
			}
		})
	}
}

func TestEncodeTrunkPage_errors(t *testing.T) {
	t.Parallel()
	t.Run("buffer_too_small", func(t *testing.T) {
		t.Parallel()
		buf := make([]byte, PageSize-1)
		err := encodeTrunkPage(trunk{}, buf)
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("too_many_intervals", func(t *testing.T) {
		t.Parallel()
		ivs := make([]PageInterval, trunkMaxIntervalsPerPage+1)
		for i := range ivs {
			ivs[i] = PageInterval{First: PageId(i), Last: PageId(i + 1)}
		}
		buf := make([]byte, PageSize)
		err := encodeTrunkPage(trunk{Intervals: ivs}, buf)
		if !errors.Is(err, ErrTrunkFull) {
			t.Fatalf("got %v, want ErrTrunkFull", err)
		}
	})
}

func TestDecodeTrunkPage_errors(t *testing.T) {
	t.Parallel()
	t.Run("buffer_too_small", func(t *testing.T) {
		t.Parallel()
		_, err := decodeTrunkPage(make([]byte, trunkHeaderSize-1))
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("bad_magic", func(t *testing.T) {
		t.Parallel()
		buf := make([]byte, PageSize)
		_, err := decodeTrunkPage(buf)
		if !errors.Is(err, ErrBadTrunkMagic) {
			t.Fatalf("got %v, want ErrBadTrunkMagic", err)
		}
	})

	t.Run("corrupt_nSeg", func(t *testing.T) {
		t.Parallel()
		buf := make([]byte, PageSize)
		if err := encodeTrunkPage(trunk{}, buf); err != nil {
			t.Fatal(err)
		}
		writeU32(buf, trunkOffNSeg, 1_000_000)
		_, err := decodeTrunkPage(buf)
		if err == nil || err.Error() == "" {
			t.Fatal("expected corrupt segment count error")
		}
	})

	t.Run("truncated_segments", func(t *testing.T) {
		t.Parallel()
		buf := make([]byte, trunkHeaderSize+8) // header + half an interval
		writeU64(buf, trunkOffMagic, TrunkPageMagic)
		writePageID(buf, trunkOffNext, EmptyTrunkID)
		writeU32(buf, trunkOffNSeg, 1)
		writeU32(buf, trunkOffPad, 0)
		_, err := decodeTrunkPage(buf)
		if err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestEmptyTrunkData_encodeDecode(t *testing.T) {
	t.Parallel()
	td := emptyTrunkData()
	buf := make([]byte, PageSize)
	if err := encodeTrunkPage(td, buf); err != nil {
		t.Fatal(err)
	}
	got, err := decodeTrunkPage(buf)
	if err != nil {
		t.Fatal(err)
	}
	if got.NextId != EmptyTrunkID || len(got.Intervals) != 0 {
		t.Fatalf("got %+v", got)
	}
}

// --- trunkIter + fake mmap ---

type trunkTestMmap struct {
	pages [][]byte
}

func newTrunkTestMmap(n int) *trunkTestMmap {
	m := &trunkTestMmap{pages: make([][]byte, n)}
	for i := range m.pages {
		m.pages[i] = make([]byte, PageSize)
	}
	return m
}

func (m *trunkTestMmap) LoadPage(id PageId) (PageHandle, error) {
	if uint64(id) >= uint64(len(m.pages)) {
		return nil, fmt.Errorf("pagealloc test: no page %d", id)
	}
	return &trunkTestHandle{id: id, buf: m.pages[id]}, nil
}

func (m *trunkTestMmap) LoadPages(iv PageInterval) ([]PageHandle, error) {
	n := iv.Length()
	if n == 0 {
		return nil, nil
	}
	out := make([]PageHandle, 0, n)
	for id := iv.First; id < iv.Last; id++ {
		ph, err := m.LoadPage(id)
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

func (m *trunkTestMmap) Resize(uint64) error { return nil }

func (m *trunkTestMmap) FilePages() (uint64, error) {
	return uint64(len(m.pages)), nil
}

func (m *trunkTestMmap) Expand(pages uint64) (PageInterval, error) {
	if pages == 0 {
		return PageInterval{}, nil
	}
	cur := uint64(len(m.pages))
	first := PageId(cur)
	for uint64(len(m.pages)) < cur+pages {
		m.pages = append(m.pages, make([]byte, PageSize))
	}
	return PageInterval{First: first, Last: first + PageId(pages)}, nil
}

type trunkTestHandle struct {
	id  PageId
	buf []byte
}

func (h *trunkTestHandle) Id() PageId   { return h.id }
func (h *trunkTestHandle) Read() []byte { return h.buf }
func (h *trunkTestHandle) Write(data []byte) {
	copy(h.buf, data)
}
func (h *trunkTestHandle) Flush() error { return nil }
func (h *trunkTestHandle) Close()       {}

func TestTrunkIter_walks_chain(t *testing.T) {
	t.Parallel()
	m := newTrunkTestMmap(4)
	td1 := trunk{NextId: 2, Intervals: []PageInterval{{10, 11}}}
	td2 := trunk{NextId: EmptyTrunkID, Intervals: []PageInterval{{20, 21}}}
	if err := encodeTrunkPage(td1, m.pages[1]); err != nil {
		t.Fatal(err)
	}
	if err := encodeTrunkPage(td2, m.pages[2]); err != nil {
		t.Fatal(err)
	}

	i := trunkIter{mmap: m, nextId: 1}
	if !i.hasNext() {
		t.Fatal("expected hasNext before walk")
	}
	h1, err := i.next()
	if err != nil {
		t.Fatal(err)
	}
	defer h1.close()
	if h1.data.NextId != 2 || !slices.Equal(h1.data.Intervals, td1.Intervals) {
		t.Fatalf("first trunk %+v", h1.data)
	}
	if !i.hasNext() {
		t.Fatal("expected hasNext before second")
	}
	h2, err := i.next()
	if err != nil {
		t.Fatal(err)
	}
	defer h2.close()
	if h2.data.NextId != EmptyTrunkID {
		t.Fatalf("second next id = %v", h2.data.NextId)
	}
	if i.hasNext() {
		t.Fatal("expected iterator exhausted")
	}
	_, err = i.next()
	if !errors.Is(err, ErrIterNoElements) {
		t.Fatalf("got %v, want ErrIterNoElements", err)
	}
}

func TestTrunkIter_next_bad_magic(t *testing.T) {
	t.Parallel()
	m := newTrunkTestMmap(2)
	i := trunkIter{mmap: m, nextId: 1}
	th, err := i.next()
	defer th.close()
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, ErrTrunkBadPage) || !errors.Is(err, ErrBadTrunkMagic) {
		t.Fatalf("got %v", err)
	}
	if i.hasNext() {
		t.Fatal("iterator should stop after bad page")
	}
}
