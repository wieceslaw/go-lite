// Package pagealloc implements a page-granular allocator over a growable file.
//
// # File layout
//
// The backing store is a sequence of fixed-size pages (see PageSize). Logical
// page index i starts at byte offset i * PageSize. All multi-byte integers on
// disk are little-endian (see le.go).
//
// A minimal database uses at least page 0:
//
//	page 0 (HeaderPageId): database header prefix, then zero padding to PageSize
//	page 1..N-1:           user data and/or free-list trunk pages
//
// The header records total page count and the first/last page IDs of the
// singly linked list of trunk pages that store free PageIntervals. An empty
// free list uses EmptyTrunkID for both trunk pointers. See header.go and
// trunk.go for exact byte layouts.
//
//	+---------+---------+---------+-----+
//	| page 0  | page 1  | page 2  | ... |
//	| header  | data /  | trunk /     |
//	| + pad   | trunk   | data        |
//	+---------+---------+---------+-----+

package pagealloc

import (
	"errors"
	"fmt"
	"sync"
)

// --- mmap ---

type MmapFile interface {
	// LoadPage guarantees the page is available for read/write (may be cached).
	LoadPage(id PageId) (PageHandle, error)
	LoadPages(iv PageInterval) ([]PageHandle, error)
	Resize(pages uint64) error
	Expand(pages uint64) (PageInterval, error) // returns allocated interval
	FilePages() (uint64, error)
}

type PageHandle interface {
	Id() PageId
	Read() []byte
	Write(data []byte)
	Flush() error
	Close()
}

// --- page allocator ---

const (
	HeaderPageId PageId = 0
)

type PageAllocator interface {
	Allocate(count uint64) ([]PageHandle, error)
	Free(interval PageInterval) error
}

type pageAllocatorImpl struct {
	mu     sync.Mutex
	mmap   MmapFile
	header *headerHandle
}

// TODO: add init flag to init and error if no flag
func NewPageAllocator(mmap MmapFile) (*pageAllocatorImpl, error) {
	headerPage, err := mmap.LoadPage(HeaderPageId)
	if err != nil {
		return nil, fmt.Errorf("pagealloc: load header page: %w", err)
	}
	// TODO: check init flag
	if !headerLooksInitialized(headerPage.Read()) {
		if err := initHeaderPage(headerPage); err != nil {
			return nil, fmt.Errorf("pagealloc: init header: %w", err)
		}
	}
	headerHandle, err := newHeaderHandle(mmap)
	if err != nil {
		return nil, err
	}

	return &pageAllocatorImpl{
		mmap:   mmap,
		header: &headerHandle,
	}, nil
}

func initHeaderPage(page PageHandle) error {
	data := make([]byte, PageSize)
	h := newInitialHeader()
	if err := encodeHeader(h, data); err != nil {
		return err
	}
	page.Write(data)
	return page.Flush()
}

func newHeaderHandle(mmap MmapFile) (headerHandle, error) {
	ph, err := mmap.LoadPage(HeaderPageId)
	if err != nil {
		return headerHandle{}, err
	}
	h, err := decodeHeader(ph.Read())
	if err != nil {
		return headerHandle{}, err
	}
	return headerHandle{
		ph,
		&h,
	}, nil
}

func (a *pageAllocatorImpl) Close() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.mmap = nil
	if a.header != nil {
		a.header.close()
		a.header = nil
	}
}

func (a *pageAllocatorImpl) Free(interval PageInterval) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if interval.IsEmpty() {
		return nil // TODO: or error?
	}

	h := a.header.data
	if err := validateFreeInterval(interval, h.Pages); err != nil {
		return err
	}

	if h.FirstTrunkPage == EmptyTrunkID {
		return a.freeFirstTrunk(interval)
	}

	return a.freeIntoTrunks(interval)
}

func validateFreeInterval(iv PageInterval, filePages uint64) error {
	if iv.First == 0 || iv.Last <= iv.First {
		return ErrInvalidPageSpan
	}
	if uint64(iv.Last) > filePages {
		return fmt.Errorf("%w: span beyond file", ErrInvalidPageSpan)
	}
	return nil
}

func (a *pageAllocatorImpl) freeFirstTrunk(iv PageInterval) error {
	trunkID, rem := iv.SplitFirst()
	ph, err := a.mmap.LoadPage(trunkID)
	if err != nil {
		return err
	}
	defer ph.Close()

	var intervals []PageInterval
	if !rem.IsEmpty() {
		intervals = []PageInterval{rem}
	}
	td := trunk{NextId: EmptyTrunkID, Intervals: intervals}
	if err := writeAndFlushTrunkPage(ph, td); err != nil {
		return err
	}

	h := a.header
	h.data.FirstTrunkPage = trunkID
	h.data.LastTrunkPage = trunkID
	return h.sync()
}

func (a *pageAllocatorImpl) freeIntoTrunks(iv PageInterval) error {
	lastID := a.header.data.LastTrunkPage
	ph, err := a.mmap.LoadPage(lastID)
	if err != nil {
		return err
	}
	defer ph.Close()

	td, err := decodeTrunkPage(ph.Read())
	if err != nil {
		return err
	}

	if mergedTd, fits := mergeIntoTrunkIfFits(td, iv); fits {
		return writeAndFlushTrunkPage(ph, mergedTd)
	}

	return a.freeIntoNewTrunkPage(ph, td, iv)
}

func (a *pageAllocatorImpl) freeIntoNewTrunkPage(lastPh PageHandle, lastTd trunk, iv PageInterval) error {
	newID, rem := iv.SplitFirst()
	newPh, err := a.mmap.LoadPage(newID)
	if err != nil {
		return err
	}
	defer newPh.Close()

	lastTd.NextId = newID
	if err := writeAndFlushTrunkPage(lastPh, lastTd); err != nil {
		return err
	}

	var newIntervals []PageInterval
	if rem.Length() > 0 {
		newIntervals = []PageInterval{rem}
	}
	newTd := trunk{NextId: EmptyTrunkID, Intervals: newIntervals}
	if err := writeAndFlushTrunkPage(newPh, newTd); err != nil {
		return err
	}

	h := a.header
	h.data.LastTrunkPage = newID
	return h.sync()
}

func (a *pageAllocatorImpl) newTrunkIter() trunkIter {
	return trunkIter{
		a.mmap,
		a.header.data.FirstTrunkPage,
	}
}

// RecoverFreeList aligns file length to header.Pages, walks the trunk chain repairing
// uninitialized trunk pages, and fixes LastTrunkPage in the header when it is wrong.
func (a *pageAllocatorImpl) recover() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.syncBackingPagesToHeader(); err != nil {
		return err
	}

	h := a.header
	if h.data.FirstTrunkPage == EmptyTrunkID {
		return nil
	}

	var last PageId
	seen := make(map[PageId]struct{})
	i := a.newTrunkIter()
	for i.hasNext() {
		cur, err := a.recoverTrunkStep(&i, seen)
		if err != nil {
			return err
		}
		last = cur
	}

	if last != h.data.LastTrunkPage {
		h.data.LastTrunkPage = last
		return h.sync()
	}
	return nil
}

func (a *pageAllocatorImpl) recoverTrunkStep(i *trunkIter, seen map[PageId]struct{}) (PageId, error) {
	th, err := i.next()
	defer th.close()

	if err != nil {
		if !errors.Is(err, ErrTrunkBadPage) {
			return 0, err
		}
	}

	cur := th.id()
	if _, dup := seen[cur]; dup {
		return 0, fmt.Errorf("pagealloc: trunk chain cycle at page %d", cur)
	}
	seen[cur] = struct{}{}

	if err != nil && errors.Is(err, ErrTrunkBadPage) {
		th.data = emptyTrunkData()
		if err := th.sync(); err != nil {
			return 0, err
		}
	}

	return cur, nil
}

func (a *pageAllocatorImpl) syncBackingPagesToHeader() error {
	n, err := a.mmap.FilePages()
	if err != nil {
		return fmt.Errorf("pagealloc: file pages: %w", err)
	}

	h := a.header
	if n == h.data.Pages {
		return nil
	}
	if err := a.mmap.Resize(h.data.Pages); err != nil {
		return fmt.Errorf("pagealloc: resize backing to %d pages: %w", h.data.Pages, err)
	}
	return nil
}

// TODO: what if program breaks between allocation and usage of page?
// Allocate prefers the on-disk free list (trunk chain); if nothing fits, grows the file.
func (a *pageAllocatorImpl) Allocate(pages uint64) ([]PageHandle, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if pages == 0 {
		return nil, nil
	}

	// TODO: change iter start ptr later for available tail (when compaction is implemented)
	i := a.newTrunkIter()
	var piv PageInterval
	for i.hasNext() {
		iv, ok, err := a.tryAllocateFromFreelist(&i, pages)
		if err != nil {
			return nil, err
		}
		if ok {
			piv = iv
			break
		}
	}

	if piv.IsEmpty() {
		iv, err := a.allocateByExpanding(pages)
		if err != nil {
			return nil, err
		}
		piv = iv
	}

	return a.mmap.LoadPages(piv)
}

func (a *pageAllocatorImpl) allocateByExpanding(pages uint64) (PageInterval, error) {
	iv, err := a.mmap.Expand(pages)
	if err != nil {
		return EmptyInterval, err
	}

	a.header.data.Pages += iv.Length()
	if err := a.header.sync(); err != nil {
		return EmptyInterval, err
	}
	return iv, nil
}

func (a *pageAllocatorImpl) tryAllocateFromFreelist(i *trunkIter, ivLen uint64) (PageInterval, bool, error) {
	th, err := i.next()
	defer th.close()
	if err != nil {
		return EmptyInterval, false, err
	}

	iv, newTr, ok := th.data.acquireInterval(ivLen)
	if !ok {
		return EmptyInterval, false, nil
	}

	th.data = newTr
	if err := th.sync(); err != nil {
		return EmptyInterval, false, err
	}
	return iv, true, nil
}

// TODO: finer-grained or lock-free allocator paths (whole-allocator mutex serializes for now)

// TODO: implement concurrent compaction alogrithm
// 1. (inmemory) switch to compaction mode - allocations and frees operations use current header.LastTrunkId as beggining (meaning all trunks before are invisible and untouched as well as their free regions)
// 		if crash happens here it won't mean anyting beacause we simply didn't use some of free space and didn't change file contents
// 2. (inmemory) compute new trunk layout - take all trunk pages (except last before switching) and their free pages, merge and sort them into new layout, with new trunks
//		if crash happens here it won't mean anyting beacause we didn't change current file layout
// 3. (in file) find space (preferably in current trunk freespace) and start putting there new layout (with last trunk pointing to old "last trunk" from p.1)
//		if crash happens here it won't mean anyting if we used current free space
// 4. (in file) atomically (under inmemory header lock) change start index in header
// 		chash can't happend in-between file sync
// 5. (inmemory) remove header lock, switch mode from compaction to default

// TODO: differentiate capacity and size of pages in allocator

// TODO: write tests that all handles are cleared
