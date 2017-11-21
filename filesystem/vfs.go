package filesystem

// this file defines the virtual file system to provide consistent file access APIs

import (
	"fmt"
	"io"
)

type OptionName string

var (
	Option = make(map[OptionName]string)
)

type FileLocation struct {
	Location string
}

type VirtualFile interface {
	io.ReaderAt
	io.ReadCloser
	io.Seeker
	Size() int64
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
		&S3FileSystem{},
	}
)

func Set(name OptionName, value string) {
	Option[name] = value
}

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
