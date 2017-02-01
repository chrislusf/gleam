// Package rsync adds file server and copying client to copy files
// between glow driver and agent.
package rsync

import (
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

type FileResource struct {
	FullPath     string `json:"path,omitempty"`
	TargetFolder string `json:"targetFolder,omitempty"`
}

type FileHash struct {
	FullPath     string `json:"path,omitempty"`
	TargetFolder string `json:"targetFolder,omitempty"`
	File         string `json:"file,omitempty"`
	Hash         uint32 `json:"hash,omitempty"`
}

func GenerateFileHash(fullpath string) (*FileHash, error) {

	if _, err := os.Stat(fullpath); os.IsNotExist(err) {
		return nil, err
	}

	f, err := os.Open(fullpath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	hasher := crc32.NewIEEE()
	if _, err := io.Copy(hasher, f); err != nil {
		return nil, err
	}
	crc := hasher.Sum32()

	return &FileHash{
		FullPath: fullpath,
		File:     filepath.Base(fullpath),
		Hash:     crc,
	}, nil
}
