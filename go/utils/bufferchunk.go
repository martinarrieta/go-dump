package utils

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

const BufferTypeGzipFile = "gzip"
const BufferTypeFile = "file"

type Buffer struct {
	Type           string
	Buffer         *bufio.Writer
	GzipWriter     *gzip.Writer
	FileDescriptor *os.File
}

func (this *Buffer) Write(b []byte) (int, error) {
	return this.Buffer.Write(b)
}

func (this *Buffer) Flush() error {
	return this.Buffer.Flush()
}

func (this *Buffer) Close() {
	this.Flush()
	switch this.Type {
	case BufferTypeGzipFile:
		this.GzipWriter.Close()
		this.FileDescriptor.Close()
	case BufferTypeFile:
		this.FileDescriptor.Close()
	}
}

func NewChunkBuffer(c *DataChunk, workerId int) *Buffer {
	tablename := c.Task.Table.GetUnescapedFullName()
	compressExt := ""

	if c.Task.TaskManager.Compress {
		compressExt = ".gz"
	}

	filename := fmt.Sprintf("%s-thread%d.sql%s", tablename, workerId, compressExt)
	fullpath := filepath.Join(c.Task.TaskManager.DestinationDir, filename)
	fileDescriptor, err := os.Create(fullpath)
	if err != nil {
		log.Fatalf("Error crating the file %s: %s", fullpath, err.Error())
	}

	if c.Task.TaskManager.Compress {
		gzipWriter, err := gzip.NewWriterLevel(fileDescriptor, c.Task.TaskManager.CompressLevel)
		if err != nil {
			log.Fatalf("Error getting gzip writer: %s", err.Error())
		}
		buffer := bufio.NewWriter(gzipWriter)
		return &Buffer{Type: BufferTypeGzipFile, Buffer: buffer, GzipWriter: gzipWriter}
	}
	buffer := bufio.NewWriter(fileDescriptor)
	return &Buffer{Type: BufferTypeFile, Buffer: buffer}
}
