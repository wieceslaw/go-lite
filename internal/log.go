package internal

import (
	"bufio"
	"io"
	"os"
)

type (
	Op []byte
)

type OpLog struct {
	FileWriter io.Writer
}

func NewOpLog(path string) *OpLog {
	file, err := os.Open(path)
	if err != nil {
		return nil
	}
	const bufferSize = 4096
	writer := bufio.NewWriterSize(file, bufferSize)

	writer.Flush()

	return &OpLog{}
}

func (l *OpLog) append(op Op) error {
	write, err := l.FileWriter.Write(op)
	if err != nil {
		return err
	}
}
