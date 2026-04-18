package pagealloc

import (
	"errors"
	"fmt"
)

const (
	headerMagic   uint64 = 0x42444554494c4f47 // ASCII "GOLITEDB"
	headerVersion uint32 = 1
	headerSize    int    = 40 // fixed on-disk header prefix on page 0

	headerOffMagic      = 0
	headerOffVersion    = 8
	headerOffPageSize   = 12
	headerOffPages      = 16
	headerOffFirstTrunk = 24
	headerOffLastTrunk  = 32
)

var (
	ErrCorruptHeader   = errors.New("pagealloc: corrupt or truncated header page")
	ErrBadMagic        = errors.New("pagealloc: bad header magic")
	ErrBadVersion      = errors.New("pagealloc: unsupported header version")
	ErrBadPageSize     = errors.New("pagealloc: header page size mismatch")
	ErrInvalidPages    = errors.New("pagealloc: invalid page count in header")
	ErrHeaderTooShort  = errors.New("pagealloc: header buffer too short")
	ErrInvalidPageSpan = errors.New("pagealloc: invalid page span")
)

type header struct {
	Magic          uint64
	Version        uint32
	PageSizeConst  uint32
	Pages          uint64
	FirstTrunkPage PageId
	LastTrunkPage  PageId
}

func newInitialHeader() header {
	return header{
		Magic:          headerMagic,
		Version:        headerVersion,
		PageSizeConst:  uint32(PageSize),
		Pages:          1,
		FirstTrunkPage: EmptyTrunkID,
		LastTrunkPage:  EmptyTrunkID,
	}
}

func encodeHeader(h header, dst []byte) error {
	if len(dst) < headerSize {
		return ErrHeaderTooShort
	}
	writeU64(dst, headerOffMagic, h.Magic)
	writeU32(dst, headerOffVersion, h.Version)
	writeU32(dst, headerOffPageSize, h.PageSizeConst)
	writeU64(dst, headerOffPages, h.Pages)
	writePageID(dst, headerOffFirstTrunk, h.FirstTrunkPage)
	writePageID(dst, headerOffLastTrunk, h.LastTrunkPage)
	return nil
}

func decodeHeader(src []byte) (header, error) {
	if len(src) < headerSize {
		return header{}, ErrCorruptHeader
	}
	h := header{
		Magic:          readU64(src, headerOffMagic),
		Version:        readU32(src, headerOffVersion),
		PageSizeConst:  readU32(src, headerOffPageSize),
		Pages:          readU64(src, headerOffPages),
		FirstTrunkPage: readPageID(src, headerOffFirstTrunk),
		LastTrunkPage:  readPageID(src, headerOffLastTrunk),
	}
	if h.Magic != headerMagic {
		return header{}, fmt.Errorf("%w", ErrBadMagic)
	}
	if h.Version != headerVersion {
		return header{}, fmt.Errorf("%w", ErrBadVersion)
	}
	if uint64(h.PageSizeConst) != PageSize {
		return header{}, ErrBadPageSize
	}
	if h.Pages < 1 {
		return header{}, ErrInvalidPages
	}
	return h, nil
}

func headerLooksInitialized(src []byte) bool {
	if len(src) < 8 {
		return false
	}
	if readU64(src, headerOffMagic) != headerMagic {
		return false
	}
	_, err := decodeHeader(src)
	return err == nil
}

// --- handle ---
type headerHandle struct {
	ph   PageHandle
	data *header
}

func (h *headerHandle) close() {
	if h.ph != nil {
		h.ph.Close()
		h.ph = nil
	}
	h.data = nil
}

func (h *headerHandle) sync() error {
	buf := make([]byte, PageSize)
	if err := encodeHeader(*h.data, buf); err != nil {
		return err
	}
	h.ph.Write(buf)
	return h.ph.Flush()
}
