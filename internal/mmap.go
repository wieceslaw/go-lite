package internal

import "os"

type PageHandle struct {
	content   []byte
	pageIndex int
	owner     *MmapFile
}

type MmapFile struct {
	file *os.File
}

// StorePage(pageIndex int, data []byte) error

func (mf *MmapFile) GetPage(pageIndex int) ([]byte, error) {
	return nil, nil
}

// loadPages returns a handle per page in iv (half-open [First, Last)).
// func (a *pageAllocatorImpl) loadPages(iv PageInterval) ([]PageHandle, error) {
// 	n := iv.Length()
// 	if n == 0 {
// 		return nil, nil
// 	}
// 	out := make([]PageHandle, 0, n)
// 	for id := iv.First; id < iv.Last; id++ {
// 		ph, err := a.mmap.LoadPage(id)
// 		if err != nil {
// 			for _, h := range out {
// 				h.Close()
// 			}
// 			return nil, err
// 		}
// 		out = append(out, ph)
// 	}
// 	return out, nil
// }
