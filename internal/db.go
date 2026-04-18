package internal

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// handle for operations with db
// resource - has state and should be closed to gracefully shutdown
type DB struct {
	File *os.File
}

type DbOpenSettings struct {
	CreateIfNotExists bool
	TruncateIfExists  bool
}

func Open(ctx context.Context, rawPath string, settings *DbOpenSettings) (*DB, error) {
	// acquire separate lock file before opening
	acquireLock(ctx, rawPath)

	file, err := openFileGracefully(rawPath, settings)
	if err != nil {
		return nil, fmt.Errorf("Failed to open file: %v", err)
	}
	return &DB{
		File: file,
	}, nil
}

func acquireLock(ctx context.Context, rawPath string) {

}

func openFileGracefully(rawPath string, settings *DbOpenSettings) (*os.File, error) {
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
		if !settings.CreateIfNotExists && os.IsNotExist(err) {
			return nil, fmt.Errorf("file does not exist: %s", absPath)
		}
		return nil, fmt.Errorf("cannot stat path: %w", err)
	}

	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("path is not a regular file: %s", absPath)
	}

	flag := os.O_RDWR
	if settings.CreateIfNotExists {
		flag = flag | os.O_CREATE
	}
	if settings.TruncateIfExists {
		flag = flag | os.O_TRUNC
	}
	file, err := os.OpenFile(absPath, flag, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	return file, nil
}

func (db *DB) Close(ctx context.Context) error {
	// TODO: gracefully shutdown db
	return nil
}
