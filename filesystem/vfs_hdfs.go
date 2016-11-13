package filesystem

import (
	"fmt"
	"log"
	"strings"

	"github.com/colinmarc/hdfs"
)

type HdfsFileSystem struct {
}

func (fs *HdfsFileSystem) Accept(fl *FileLocation) bool {
	return strings.HasPrefix(fl.Location, "hdfs://")
}

func (fs *HdfsFileSystem) Open(fl *FileLocation) (VirtualFile, error) {
	namenode, path, err := splitLocationToParts(fl.Location)
	if err != nil {
		return nil, err
	}

	client, err := hdfs.New(namenode)
	if err != nil {
		log.Fatalf("failed to create client to %s:%v\n", namenode, err)
	}

	file, err := client.Open(path)

	return file, err
}

// List generates a full list of file locations under the given
// location, which should have a prefix of hdfs://
func (fs *HdfsFileSystem) List(fl *FileLocation) (fileLocations []*FileLocation, err error) {
	namenode, path, err := splitLocationToParts(fl.Location)
	if err != nil {
		return
	}

	client, err := hdfs.New(namenode)
	if err != nil {
		return nil, fmt.Errorf("failed to create client to %s:%v\n", namenode, err)
	}

	fileInfos, err := client.ReadDir("/" + path)
	if err != nil {
		return nil, fmt.Errorf("failed to list files under /%s:%v\n", path, err)
	}

	for _, fi := range fileInfos {
		fileLocations = append(fileLocations, &FileLocation{fl.Location + "/" + fi.Name()})
	}

	return
}

func (fs *HdfsFileSystem) IsDir(fl *FileLocation) bool {
	namenode, path, err := splitLocationToParts(fl.Location)
	if err != nil {
		log.Fatalf("failed to create client to %s:%v\n", namenode, err)
		return false
	}

	client, err := hdfs.New(namenode)
	if err != nil {
		log.Fatalf("failed to create client to %s:%v\n", namenode, err)
	}

	file, err := client.Open(path)
	if err != nil {
		log.Fatalf("failed to open file %s:%v\n", fl.Location, err)
	}

	defer file.Close()

	fi := file.Stat()

	return fi.IsDir()
}

func splitLocationToParts(location string) (namenode, path string, err error) {
	hdfsPrefix := "hdfs://"
	if !strings.HasPrefix(location, hdfsPrefix) {
		return "", "", fmt.Errorf("parameter %s should start with hdfs://", location)
	}

	parts := strings.SplitN(location[len(hdfsPrefix):], "/", 2)
	return parts[0], "/" + parts[1], nil
}
