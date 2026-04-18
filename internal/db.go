package internal

import (
	"Golite/internal/mmap"
	"Golite/internal/pagealloc"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// handle for operations with db
// resource - has state and should be closed to gracefully shutdown
type DB struct {
	File      *os.File
	MmapFile  pagealloc.MmapFile
	Allocator pagealloc.PageAllocator
}

type OpenSettings struct {
	CreateFileIfNotExists bool
	TruncateFileIfExists  bool
	RecoverFileOnOpening  bool
}

// TODO: improve error interface
func Open(ctx context.Context, rawPath string, settings *OpenSettings) (*DB, error) {
	// TODO: make refactoring such that resources won't be forgetted to close on error in creating one of them
	err := acquireFileLock(ctx, rawPath) // TODO: also return some lock handle
	if err != nil {
		return nil, err
	}

	file, err := openFileGracefully(rawPath, settings)
	if err != nil {
		// TODO: unlock file
		return nil, fmt.Errorf("Failed to open file: %v", err)
	}

	mmapFile, err := mmap.NewFile(file)
	if err != nil {
		// TODO: unlock file
		file.Close()
		return nil, fmt.Errorf("Failed to mmap file: %v", err)
	}

	allocator, err := pagealloc.NewPageAllocator(mmapFile)
	if err != nil {
		// TODO: close everything
		return nil, fmt.Errorf("Failed to mmap file: %v", err)
	}

	return &DB{
		File:      file,
		MmapFile:  mmapFile,
		Allocator: allocator,
	}, nil
}

func acquireFileLock(ctx context.Context, rawPath string) error {
	// TODO: Implement
	return nil
}

func openFileGracefully(rawPath string, settings *OpenSettings) (*os.File, error) {
	trimmed := strings.TrimSpace(rawPath)
	if trimmed == "" {
		return nil, fmt.Errorf("invalid path: path is empty or whitespace")
	}

	if strings.ContainsRune(trimmed, '\x00') {
		return nil, fmt.Errorf("invalid path: contains null byte")
	}

	cleanPath := filepath.Clean(trimmed)

	absPath, err := filepath.Abs(cleanPath)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve absolute path: %w", err)
	}

	info, err := os.Stat(absPath)
	if err != nil {
		if !settings.CreateFileIfNotExists && os.IsNotExist(err) {
			return nil, fmt.Errorf("file does not exist: %s", absPath)
		}
		return nil, fmt.Errorf("cannot stat path: %w", err)
	}

	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("path is not a regular file: %s", absPath)
	}

	flag := os.O_RDWR
	if settings.CreateFileIfNotExists {
		flag = flag | os.O_CREATE
	}
	if settings.TruncateFileIfExists {
		flag = flag | os.O_TRUNC
	}
	file, err := os.OpenFile(absPath, flag, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	return file, nil
}

func (db *DB) Close(ctx context.Context) error {
	// TODO: free other resources (not stopping on single error)
	if err := db.File.Close(); err != nil {
		return err
	}
	return nil
}
