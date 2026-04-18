package pagealloc

import (
	"errors"
	"fmt"
	"math"
	"sort"
)

const (
	EmptyTrunkID PageId = PageId(math.MaxUint64)

	// TrunkPageMagic identifies a free-list trunk page (distinct from headerMagic on page 0).
	// Tools can scan page-aligned buffers for this value to recover trunk chain structure.
	TrunkPageMagic uint64 = 0x4b525454494c4f47 // ASCII "GOLITTRK"

	trunkHeaderSize   = 24 // magic(8) + next(8) + nSeg(4) + pad(4)
	trunkIntervalSize = 16 // First(8) + Last(8), Last exclusive

	trunkOffMagic = 0
	trunkOffNext  = 8
	trunkOffNSeg  = 16
	trunkOffPad   = 20

	// trunkMaxIntervalsPerPage is how many PageInterval slots fit in one page (see encodeTrunkPage).
	trunkMaxIntervalsPerPage = (PageSize - trunkHeaderSize) / trunkIntervalSize
)

var (
	ErrTrunkFull     = errors.New("pagealloc: trunk page has no room for another segment")
	ErrBadTrunkMagic = errors.New("pagealloc: bad trunk page magic")

	ErrIterNoElements = errors.New("No elements in iterator")
	ErrTrunkBadPage   = errors.New("pagealloc: unable to deserialize trunk page")
)

type trunk struct {
	NextId    PageId
	Intervals []PageInterval
}

func emptyTrunkData(id PageId) trunk {
	return trunk{
		NextId:    EmptyTrunkID,
		Intervals: nil,
	}
}

func writeAndFlushTrunkPage(ph PageHandle, td trunk) error {
	buf := make([]byte, PageSize)
	if err := encodeTrunkPage(td, buf); err != nil {
		return err
	}
	ph.Write(buf)
	return ph.Flush()
}

func decodeTrunkPage(buf []byte) (trunk, error) {
	if len(buf) < trunkHeaderSize {
		return trunk{}, errors.New("pagealloc: buffer is too small")
	}
	if readU64(buf, trunkOffMagic) != TrunkPageMagic {
		return trunk{}, fmt.Errorf("%w", ErrBadTrunkMagic)
	}
	next := readPageID(buf, trunkOffNext)
	n := readU32(buf, trunkOffNSeg)
	maxSeg := (len(buf) - trunkHeaderSize) / trunkIntervalSize
	if n > uint32(maxSeg) {
		return trunk{}, errors.New("pagealloc: corrupt trunk segment count")
	}
	intervals := make([]PageInterval, 0, n)
	off := trunkHeaderSize
	for i := uint32(0); i < n; i++ {
		if off+trunkIntervalSize > len(buf) {
			return trunk{}, errors.New("pagealloc: corrupt trunk segments")
		}
		intervals = append(intervals, readPageInterval(buf, off))
		off += trunkIntervalSize
	}
	return trunk{NextId: next, Intervals: intervals}, nil
}

func encodeTrunkPage(td trunk, dst []byte) error {
	if len(dst) < int(PageSize) {
		return errors.New("pagealloc: trunk encode buffer too small")
	}
	n := len(td.Intervals)
	maxSeg := (len(dst) - trunkHeaderSize) / trunkIntervalSize
	if n > maxSeg {
		return ErrTrunkFull
	}
	writeU64(dst, trunkOffMagic, TrunkPageMagic)
	writePageID(dst, trunkOffNext, td.NextId)
	writeU32(dst, trunkOffNSeg, uint32(n))
	writeU32(dst, trunkOffPad, 0)
	off := trunkHeaderSize
	for _, s := range td.Intervals {
		writePageInterval(dst, off, s)
		off += trunkIntervalSize
	}
	// rest left as zero from caller if needed
	return nil
}

// mergeIntoTrunkIfFits returns a TrunkData with iv merged into td.Intervals (same NextTrunk),
// or ok false if the merged intervals would not fit on one trunk page. Segment count can stay
// the same or shrink when iv adjoins existing intervals.
func mergeIntoTrunkIfFits(td trunk, iv PageInterval) (trunk, bool) {
	ivs := append([]PageInterval(nil), td.Intervals...)
	merged := mergeItervals(ivs, iv)
	if len(merged) > int(trunkMaxIntervalsPerPage) {
		return trunk{}, false
	}
	return trunk{NextId: td.NextId, Intervals: merged}, true
}

// adds interval to interval list, sorting and merging them
func mergeItervals(ivs []PageInterval, add PageInterval) []PageInterval {
	ivs = append(ivs, add)
	sortIntervalsByStart(ivs)
	return mergeAdjacentSorted(ivs)
}

func sortIntervalsByStart(ivs []PageInterval) {
	sort.Slice(ivs, func(i, j int) bool {
		return ivs[i].First < ivs[j].First
	})
}

func mergeAdjacentSorted(ivs []PageInterval) []PageInterval {
	if len(ivs) <= 1 {
		return ivs
	}
	out := make([]PageInterval, 0, len(ivs))
	cur := ivs[0]
	for i := 1; i < len(ivs); i++ {
		s := ivs[i]
		if s.First <= cur.Last {
			if s.Last > cur.Last {
				cur.Last = s.Last
			}
			continue
		}
		out = append(out, cur)
		cur = s
	}
	out = append(out, cur)
	return out
}

// --- iterator ---
type trunkIter struct {
	mmap   MmapFile
	nextId PageId
}

type trunkHandle struct {
	ph   PageHandle
	data trunk
}

func (th *trunkHandle) id() PageId {
	return th.ph.Id()
}

func (th *trunkHandle) close() {
	if th.ph != nil {
		th.ph.Close()
	}
	th.ph = nil
	th.data = trunk{}
}

func (th *trunkHandle) sync() error {
	return writeAndFlushTrunkPage(th.ph, th.data)
}

func (i *trunkIter) next() (trunkHandle, error) {
	if i.nextId == EmptyTrunkID {
		return trunkHandle{}, ErrIterNoElements
	}

	ph, err := i.mmap.LoadPage(i.nextId)
	if err != nil {
		i.nextId = EmptyTrunkID
		return trunkHandle{}, err
	}

	td, err := decodeTrunkPage(ph.Read())
	if err != nil {
		i.nextId = EmptyTrunkID
		return trunkHandle{ph, trunk{}}, errors.Join(ErrTrunkBadPage, err)
	}

	i.nextId = td.NextId

	return trunkHandle{ph, td}, nil
}

func (i *trunkIter) hasNext() bool {
	return i.nextId != EmptyTrunkID
}
