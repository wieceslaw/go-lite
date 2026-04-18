package pagealloc

import "encoding/binary"

// Little-endian helpers for on-disk layouts (Header, trunk pages, and any future records).
// All multi-byte fields in this package use these primitives.

func readU64(src []byte, off int) uint64 {
	return binary.LittleEndian.Uint64(src[off : off+8])
}

func writeU64(dst []byte, off int, v uint64) {
	binary.LittleEndian.PutUint64(dst[off:off+8], v)
}

func readU32(src []byte, off int) uint32 {
	return binary.LittleEndian.Uint32(src[off : off+4])
}

func writeU32(dst []byte, off int, v uint32) {
	binary.LittleEndian.PutUint32(dst[off:off+4], v)
}

func readPageID(src []byte, off int) PageId {
	return PageId(readU64(src, off))
}

func writePageID(dst []byte, off int, id PageId) {
	writeU64(dst, off, uint64(id))
}

func readPageInterval(src []byte, off int) PageInterval {
	return PageInterval{
		First: readPageID(src, off),
		Last:  readPageID(src, off+8),
	}
}

func writePageInterval(dst []byte, off int, iv PageInterval) {
	writePageID(dst, off, iv.First)
	writePageID(dst, off+8, iv.Last)
}
