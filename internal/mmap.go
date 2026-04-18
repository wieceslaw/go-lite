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
