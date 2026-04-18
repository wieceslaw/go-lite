package internal

import (
	"io"
	"os"

	"Golite/internal/pagealloc"
)

// PageFile implements pagealloc.MmapFile using the OS file as a page-aligned store (read/write at offsets).
type PageFile struct {
	f *os.File
}

func NewPageFile(f *os.File) *PageFile {
	return &PageFile{f: f}
}

var _ pagealloc.MmapFile = (*PageFile)(nil)

func (p *PageFile) Resize(pages uint64) error {
	size := int64(uint64(pagealloc.PageSize) * pages)
	return p.f.Truncate(size)
}

func (p *PageFile) FilePages() (uint64, error) {
	st, err := p.f.Stat()
	if err != nil {
		return 0, err
	}
	sz := st.Size()
	if sz <= 0 {
		return 0, nil
	}
	return (uint64(sz) + pagealloc.PageSize - 1) / pagealloc.PageSize, nil
}

// Expand grows the backing file by pages and returns the newly added half-open page interval.
func (p *PageFile) Expand(pages uint64) (pagealloc.PageInterval, error) {
	if pages == 0 {
		return pagealloc.PageInterval{}, nil
	}
	cur, err := p.FilePages()
	if err != nil {
		return pagealloc.PageInterval{}, err
	}
	first := pagealloc.PageId(cur)
	if err := p.Resize(cur + pages); err != nil {
		return pagealloc.PageInterval{}, err
	}
	return pagealloc.PageInterval{First: first, Last: first + pagealloc.PageId(pages)}, nil
}

func (p *PageFile) LoadPage(id pagealloc.PageId) (pagealloc.PageHandle, error) {
	off := int64(id) * int64(pagealloc.PageSize)
	buf := make([]byte, pagealloc.PageSize)
	n, err := p.f.ReadAt(buf, off)
	if err != nil && err != io.EOF {
		return nil, err
	}
	if n < len(buf) {
		for i := n; i < len(buf); i++ {
			buf[i] = 0
		}
	}
	return &pageFileHandle{f: p.f, id: id, off: off, data: buf}, nil
}

func (p *PageFile) LoadPages(iv pagealloc.PageInterval) ([]pagealloc.PageHandle, error) {
	n := iv.Length()
	if n == 0 {
		return nil, nil
	}
	out := make([]pagealloc.PageHandle, 0, n)
	for id := iv.First; id < iv.Last; id++ {
		ph, err := p.LoadPage(id)
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

type pageFileHandle struct {
	f    *os.File
	id   pagealloc.PageId
	off  int64
	data []byte
}

func (h *pageFileHandle) Id() pagealloc.PageId { return h.id }

func (h *pageFileHandle) Read() []byte { return h.data }

func (h *pageFileHandle) Write(data []byte) {
	n := copy(h.data, data)
	for i := n; i < len(h.data); i++ {
		h.data[i] = 0
	}
}

func (h *pageFileHandle) Flush() error {
	_, err := h.f.WriteAt(h.data, h.off)
	return err
}

func (h *pageFileHandle) Close() {}
