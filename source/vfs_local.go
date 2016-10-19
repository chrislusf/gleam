package source

// this file defines the virtual file system to provide consistent file access APIs

import (
	"io/ioutil"
	"os"
	"strings"
)

type LocalFileSystem struct {
}

func (fs *LocalFileSystem) Accept(fl *FileLocation) bool {
	return !strings.HasPrefix(fl.Location, "hdfs://")
}

func (fs *LocalFileSystem) Open(fl *FileLocation) (VirtualFile, error) {
	osFile, err := os.Open(fl.Location)
	return osFile, err
}

func (fs *LocalFileSystem) List(fl *FileLocation) (fileLocations []*FileLocation, err error) {
	files, err := ioutil.ReadDir(fl.Location)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		fileLocations = append(fileLocations, &FileLocation{fl.Location + "/" + file.Name()})
	}
	return
}
