package csv

import (
	"fmt"
	"path/filepath"

	"github.com/chrislusf/gleam/adapter"
	"github.com/chrislusf/gleam/filesystem"
)

func (c *CsvAdapter) GetSplits(connectionId string, aq adapter.AdapterQuery) (splits []adapter.Split, err error) {

	s, isCsvSource := aq.(Source)
	if !isCsvSource {
		return nil, fmt.Errorf("input for GetSplits() is not csv source? %v", aq)
	}

	connectionInfo, ok := adapter.ConnectionManager.GetConnectionInfo(connectionId)
	if !ok {
		return nil, fmt.Errorf("Failed to find configuration for %s.", connectionId)
	}
	c.LoadConfiguration(connectionInfo.GetConfig())

	if !s.hasWildcard && !filesystem.IsDir(s.Path) {
		splits = append(splits, &CsvDataSplit{
			FileName:  s.Path,
			HasHeader: s.HasHeader,
		})
	} else {
		virtualFiles, err := filesystem.List(s.folder)
		if err != nil {
			return nil, fmt.Errorf("Failed to list folder %s: %v", s.folder, err)
		}
		for _, vf := range virtualFiles {
			if !s.hasWildcard || s.Match(vf.Location) {
				splits = append(splits, &CsvDataSplit{
					FileName:  vf.Location,
					HasHeader: s.HasHeader,
				})
			}
		}
	}

	return
}

func (ci *Source) Match(fullPath string) bool {
	baseName := filepath.Base(fullPath)
	match, _ := filepath.Match(ci.fileBaseName, baseName)
	return match
}
