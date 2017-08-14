package filesystem

import (
	"io/ioutil"
	"log"
	"os"
	"strings"
)

type LocalFileSystem struct {
}

func (fs *LocalFileSystem) Accept(fl *FileLocation) bool {
	return !strings.HasPrefix(fl.Location, "hdfs://") && !strings.HasPrefix(fl.Location, "s3://")
}

func (fs *LocalFileSystem) Open(fl *FileLocation) (VirtualFile, error) {
	osFile, err := os.Open(fl.Location)
	return &VirtualFileLocal{osFile}, err
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

func (fs *LocalFileSystem) IsDir(fl *FileLocation) bool {
	f, err := os.Open(fl.Location)
	if err != nil {
		log.Println(err)
		return false
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		log.Println(err)
		return false
	}
	switch mode := fi.Mode(); {
	case mode.IsDir():
		return true
	case mode.IsRegular():
		return false
	}
	return false
}

type VirtualFileLocal struct {
	*os.File
}

func (vf *VirtualFileLocal) Size() int64 {
	fileInfo, err := vf.File.Stat()
	if err != nil {
		return 0
	}
	return fileInfo.Size()
}
