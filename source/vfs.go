package source

// this file defines the virtual file system to provide consistent file access APIs

import (
	"fmt"
	"io"
)

type FileLocation struct {
	Location string
}

type VirtualFile interface {
	io.ReaderAt
	io.ReadCloser
}

type VirtualFileSystem interface {
	Accept(*FileLocation) bool
	Open(*FileLocation) (VirtualFile, error)
	List(*FileLocation) ([]*FileLocation, error)
	IsDir(*FileLocation) bool
}

var (
	fileSystems = []VirtualFileSystem{
		&LocalFileSystem{},
		&HdfsFileSystem{},
	}
)

func Open(filepath string) (VirtualFile, error) {
	fileLocation := &FileLocation{filepath}
	for _, fs := range fileSystems {
		if fs.Accept(fileLocation) {
			return fs.Open(fileLocation)
		}
	}
	return nil, fmt.Errorf("Unknown file %s", filepath)
}

func List(filepath string) ([]*FileLocation, error) {
	fileLocation := &FileLocation{filepath}
	for _, fs := range fileSystems {
		if fs.Accept(fileLocation) {
			return fs.List(fileLocation)
		}
	}
	return nil, fmt.Errorf("Unknown file %s", filepath)
}

func IsDir(filepath string) bool {
	fileLocation := &FileLocation{filepath}
	for _, fs := range fileSystems {
		if fs.Accept(fileLocation) {
			return fs.IsDir(fileLocation)
		}
	}
	return false
}
