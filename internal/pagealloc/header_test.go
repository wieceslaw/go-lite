package pagealloc

import (
	"errors"
	"testing"
)

func TestEncodeDecodeHeader_roundTrip(t *testing.T) {
	t.Parallel()
	h := header{
		Magic:          headerMagic,
		Version:        headerVersion,
		PageSizeConst:  uint32(PageSize),
		Pages:          42,
		FirstTrunkPage: 7,
		LastTrunkPage:  9,
	}
	buf := make([]byte, PageSize)
	if err := encodeHeader(h, buf); err != nil {
		t.Fatal(err)
	}
	got, err := decodeHeader(buf)
	if err != nil {
		t.Fatal(err)
	}
	if got != h {
		t.Fatalf("decode = %+v, want %+v", got, h)
	}
}

func TestEncodeHeader_bufferTooShort(t *testing.T) {
	t.Parallel()
	h := newInitialHeader()
	buf := make([]byte, headerSize-1)
	err := encodeHeader(h, buf)
	if !errors.Is(err, ErrHeaderTooShort) {
		t.Fatalf("got %v, want ErrHeaderTooShort", err)
	}
}

func TestDecodeHeader_errors(t *testing.T) {
	t.Parallel()
	t.Run("truncated", func(t *testing.T) {
		t.Parallel()
		_, err := decodeHeader(make([]byte, headerSize-1))
		if !errors.Is(err, ErrCorruptHeader) {
			t.Fatalf("got %v, want ErrCorruptHeader", err)
		}
	})
	t.Run("bad_magic", func(t *testing.T) {
		t.Parallel()
		buf := make([]byte, PageSize)
		h := newInitialHeader()
		if err := encodeHeader(h, buf); err != nil {
			t.Fatal(err)
		}
		buf[0] ^= 0xff
		_, err := decodeHeader(buf)
		if !errors.Is(err, ErrBadMagic) {
			t.Fatalf("got %v, want ErrBadMagic", err)
		}
	})
	t.Run("bad_version", func(t *testing.T) {
		t.Parallel()
		buf := make([]byte, PageSize)
		h := newInitialHeader()
		if err := encodeHeader(h, buf); err != nil {
			t.Fatal(err)
		}
		writeU32(buf, headerOffVersion, 999)
		_, err := decodeHeader(buf)
		if !errors.Is(err, ErrBadVersion) {
			t.Fatalf("got %v, want ErrBadVersion", err)
		}
	})
	t.Run("bad_page_size", func(t *testing.T) {
		t.Parallel()
		buf := make([]byte, PageSize)
		h := newInitialHeader()
		if err := encodeHeader(h, buf); err != nil {
			t.Fatal(err)
		}
		writeU32(buf, headerOffPageSize, 123)
		_, err := decodeHeader(buf)
		if !errors.Is(err, ErrBadPageSize) {
			t.Fatalf("got %v, want ErrBadPageSize", err)
		}
	})
	t.Run("invalid_pages_zero", func(t *testing.T) {
		t.Parallel()
		buf := make([]byte, PageSize)
		h := newInitialHeader()
		if err := encodeHeader(h, buf); err != nil {
			t.Fatal(err)
		}
		writeU64(buf, headerOffPages, 0)
		_, err := decodeHeader(buf)
		if !errors.Is(err, ErrInvalidPages) {
			t.Fatalf("got %v, want ErrInvalidPages", err)
		}
	})
}
