//go:build unix

package mmap

import (
	"errors"
	"fmt"
	"os"
	"syscall"
	"unsafe"

	"Golite/internal/pagealloc"
)

const (
	// TODO: 32mb?
	MmapSize uint64 = 32 * 1024 // 32KB
)

var _ pagealloc.MmapFile = (*mmapFileImpl)(nil)

type mmapFileImpl struct {
	f    *os.File
	data []byte
}

// NewFile returns a page-granular mmap view of f. f must be opened read/write.
// The file length must be zero or a multiple of pagealloc.PageSize.
func NewFile(f *os.File) (pagealloc.MmapFile, error) {
	m := &mmapFileImpl{f: f}
	if err := m.remap(); err != nil {
		return nil, err
	}
	return m, nil
}

func (m *mmapFileImpl) LoadPage(id pagealloc.PageId) (pagealloc.PageHandle, error) {
	// finds or creates mapping from mapppings cache
	// creates slice of mapping
	// creates handle and saves handle inside
	// handle close should signal cache that mapping is no longer used
	// close all handles on shutdown

	return nil, nil
}

func (m *mmapFileImpl) LoadPages(iv pagealloc.PageInterval) ([]pagealloc.PageHandle, error) {
	n := iv.Length()
	if n == 0 {
		return nil, nil
	}
	out := make([]pagealloc.PageHandle, 0, n)
	for id := iv.First; id < iv.Last; id++ {
		ph, err := m.loadPageLocked(id)
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

func (m *mmapFileImpl) loadPageLocked(id pagealloc.PageId) (pagealloc.PageHandle, error) {
	var n uint64
	if len(m.data) > 0 {
		n = uint64(len(m.data)) / pagealloc.PageSize
	}
	if uint64(id) >= n {
		return nil, os.ErrNotExist
	}
	off := uint64(id) * pagealloc.PageSize
	buf := make([]byte, pagealloc.PageSize)
	copy(buf, m.data[off:off+pagealloc.PageSize])
	return &pageHandle{m: m, id: id, data: buf}, nil
}

func (m *mmapFileImpl) FilePages() (uint64, error) {
	if len(m.data) == 0 {
		return 0, nil
	}
	return uint64(len(m.data)) / pagealloc.PageSize, nil
}

func (m *mmapFileImpl) Resize(pages uint64) error {
	newSize := int64(pages) * int64(pagealloc.PageSize)
	if err := m.f.Truncate(newSize); err != nil {
		return err
	}
	return m.remap()
}

func (m *mmapFileImpl) Expand(pages uint64) (pagealloc.PageInterval, error) {
	if pages == 0 {
		return pagealloc.PageInterval{}, nil
	}
	var cur uint64
	if len(m.data) > 0 {
		cur = uint64(len(m.data)) / pagealloc.PageSize
	}
	first := pagealloc.PageId(cur)
	if err := m.Resize(cur + pages); err != nil {
		return pagealloc.PageInterval{}, err
	}
	return pagealloc.PageInterval{
		First: first,
		Last:  first + pagealloc.PageId(pages),
	}, nil
}

func (m *mmapFileImpl) remap() error {
	if len(m.data) != 0 {
		err := syscall.Munmap(m.data)
		m.data = nil
		if err != nil {
			return err
		}
	}
	st, err := m.f.Stat()
	if err != nil {
		return err
	}
	sz := st.Size()
	if sz == 0 {
		return nil
	}
	ps := int64(pagealloc.PageSize)
	if sz%ps != 0 {
		return fmt.Errorf("mmap: file size %d not a multiple of page size %d", sz, ps)
	}
	data, err := syscall.Mmap(int(m.f.Fd()), 0, int(sz), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return err
	}
	m.data = data
	return nil
}

type pageHandle struct {
	m    *mmapFileImpl
	id   pagealloc.PageId
	data []byte
}

func (h *pageHandle) Id() pagealloc.PageId { return h.id }

func (h *pageHandle) Read() []byte { return h.data }

func (h *pageHandle) Write(data []byte) {
	n := copy(h.data, data)
	for i := n; i < len(h.data); i++ {
		h.data[i] = 0
	}
}

func (h *pageHandle) Flush() error {
	off := uint64(h.id) * pagealloc.PageSize
	if off+pagealloc.PageSize > uint64(len(h.m.data)) {
		return errors.New("mmap: flush past end of mapped file")
	}
	copy(h.m.data[off:off+pagealloc.PageSize], h.data)
	return msync(h.m.data[off:off+pagealloc.PageSize], syscall.MS_SYNC)
}

// msync calls SYS_MSYNC (syscall.Msync is not available on Darwin).
func msync(b []byte, flags int) error {
	if len(b) == 0 {
		return nil
	}
	_, _, e := syscall.Syscall(
		syscall.SYS_MSYNC,
		uintptr(unsafe.Pointer(&b[0])),
		uintptr(len(b)),
		uintptr(flags),
	)
	if e != 0 {
		return e
	}
	return nil
}

func (h *pageHandle) Close() {}
