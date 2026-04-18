package pagealloc

import (
	"errors"
	"testing"
)

func TestPageInterval_Length(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		iv   PageInterval
		want uint64
	}{
		{"empty", PageInterval{First: 1, Last: 1}, 0},
		{"inverted", PageInterval{First: 5, Last: 3}, 0},
		{"single", PageInterval{First: 2, Last: 3}, 1},
		{"multi", PageInterval{First: 0, Last: 10}, 10},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.iv.Length(); got != tc.want {
				t.Fatalf("Length() = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestPageInterval_IsEmpty(t *testing.T) {
	t.Parallel()
	if !(PageInterval{First: 3, Last: 3}.IsEmpty()) {
		t.Fatal("same First and Last should be empty")
	}
	if (PageInterval{First: 1, Last: 2}.IsEmpty()) {
		t.Fatal("non-empty interval reported empty")
	}
}

func TestPageInterval_SplitBySize(t *testing.T) {
	t.Parallel()
	t.Run("exact", func(t *testing.T) {
		t.Parallel()
		iv := PageInterval{First: 2, Last: 5}
		head, tail, ok := iv.SplitBySize(3)
		if !ok {
			t.Fatal("expected ok")
		}
		if head != (PageInterval{First: 2, Last: 5}) || !tail.IsEmpty() {
			t.Fatalf("head=%+v tail=%+v", head, tail)
		}
	})
	t.Run("prefix_and_remainder", func(t *testing.T) {
		t.Parallel()
		iv := PageInterval{First: 10, Last: 20}
		head, tail, ok := iv.SplitBySize(4)
		if !ok {
			t.Fatal("expected ok")
		}
		if head != (PageInterval{First: 10, Last: 14}) || tail != (PageInterval{First: 14, Last: 20}) {
			t.Fatalf("head=%+v tail=%+v", head, tail)
		}
	})
	t.Run("n_zero", func(t *testing.T) {
		t.Parallel()
		_, _, ok := PageInterval{First: 1, Last: 3}.SplitBySize(0)
		if ok {
			t.Fatal("expected !ok")
		}
	})
	t.Run("n_too_large", func(t *testing.T) {
		t.Parallel()
		_, _, ok := PageInterval{First: 1, Last: 3}.SplitBySize(5)
		if ok {
			t.Fatal("expected !ok")
		}
	})
}

func TestPageInterval_SplitFirst(t *testing.T) {
	t.Parallel()
	t.Run("empty_yields_inverted_remainder", func(t *testing.T) {
		t.Parallel()
		iv := PageInterval{First: 5, Last: 5}
		first, rest := iv.SplitFirst()
		if first != 5 || rest.First != 6 || rest.Last != 5 {
			t.Fatalf("first=%d rest=%+v", first, rest)
		}
	})
	t.Run("single_page", func(t *testing.T) {
		t.Parallel()
		iv := PageInterval{First: 7, Last: 8}
		first, rest := iv.SplitFirst()
		if first != 7 || rest.First != 8 || rest.Last != 8 {
			t.Fatalf("first=%d rest=%+v", first, rest)
		}
	})
	t.Run("multi_page", func(t *testing.T) {
		t.Parallel()
		iv := PageInterval{First: 1, Last: 4}
		first, rest := iv.SplitFirst()
		if first != 1 || rest.First != 2 || rest.Last != 4 {
			t.Fatalf("first=%d rest=%+v", first, rest)
		}
	})
}

func TestValidateFreeInterval(t *testing.T) {
	t.Parallel()
	filePages := uint64(10)
	cases := []struct {
		name string
		iv   PageInterval
		want error
	}{
		{"first_zero", PageInterval{First: 0, Last: 1}, ErrInvalidPageSpan},
		{"last_lte_first", PageInterval{First: 2, Last: 2}, ErrInvalidPageSpan},
		{"inverted", PageInterval{First: 5, Last: 3}, ErrInvalidPageSpan},
		{"beyond_file", PageInterval{First: 1, Last: 11}, ErrInvalidPageSpan},
		{"at_boundary_ok", PageInterval{First: 9, Last: 10}, nil},
		{"success", PageInterval{First: 1, Last: 3}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := validateFreeInterval(tc.iv, filePages)
			if tc.want == nil {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("expected error")
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want wrap %v", err, tc.want)
			}
		})
	}
}
