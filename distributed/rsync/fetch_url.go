package rsync

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"sync"

	"github.com/chrislusf/gleam/util"
)

var fetchLock sync.Mutex

type ListFileResult struct {
	Files []FileHash `json:"files,omitempty"`
}

func ListFiles(server string) ([]FileHash, error) {
	jsonBlob, err := util.Get(util.SchemePrefix + server + "/list")
	if err != nil {
		return nil, err
	}
	var ret ListFileResult
	err = json.Unmarshal(jsonBlob, &ret)
	if err != nil {
		return nil, err
	}
	return ret.Files, nil
}

func FetchFilesTo(driverAddress string, dir string) error {
	fetchLock.Lock()
	defer fetchLock.Unlock()

	fileList, err := ListFiles(driverAddress)
	if err != nil {
		return fmt.Errorf("Failed to list files: %v", err)
	}

	for _, fh := range fileList {
		toFile := filepath.Join(dir, filepath.Base(fh.File))
		hasSameHash := false
		if toFileHash, err := GenerateFileHash(toFile); err == nil {
			hasSameHash = toFileHash.Hash == fh.Hash
		}
		if hasSameHash {
			// println("skip downloading same", fh.File)
			continue
		}
		if err = FetchUrl(fmt.Sprintf("%s%s/file/%d", util.SchemePrefix, driverAddress, fh.Hash), toFile); err != nil {
			return fmt.Errorf("Failed to download file %s: %v", fh.File, err)
		}
	}

	return err
}

func FetchUrl(fileUrl string, destFile string) error {
	_, buf, err := util.DownloadUrl(fileUrl)
	if err != nil {
		return fmt.Errorf("Failed to read from %s: %v", fileUrl, err)
	}
	err = ioutil.WriteFile(destFile, buf, 0755)
	if err != nil {
		return fmt.Errorf("Failed to write %s: %v", destFile, err)
	}
	return nil
}
