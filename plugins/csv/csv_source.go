/*
This serves as an example on how to implement a plugin to read external data.

Usually a data set consists of many data shards.

So an input plugin has 3 steps:
1) generate a list of shard info. this runs on driver.
2) send each piece of shard info to an remote executor
3) Each executor fetch external data according to the shard info.
   Each shard info is processed by a mapper.

The shard info should be serializable/deserializable.
Usually just need to use gob to serialize and deserialize it.

Since the mapper to process shard info is in Go, the call to "gio.Init()"
is required.

*/
package csv

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/chrislusf/gleam/filesystem"
	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/util"
)

type CsvSource struct {
	folder         string
	fileBaseName   string
	hasWildcard    bool
	Path           string
	HasHeader      bool
	PartitionCount int
}

// Generate generates data shard info,
// partitions them via round robin,
// and reads each shard on each executor
func (s *CsvSource) Generate(f *flow.Flow) *flow.Dataset {
	return s.genShardInfos(f).RoundRobin(s.PartitionCount).Mapper(MapperReadShard)
}

// New creates a CsvSource based on a file name.
// The base file name can have "*", "?" pattern denoting a list of file names.
func New(fileOrPattern string, partitionCount int) *CsvSource {
	s := &CsvSource{
		PartitionCount: partitionCount,
	}

	if strings.ContainsAny(fileOrPattern, "/\\") {
		s.folder = filepath.Dir(fileOrPattern)
		s.fileBaseName = filepath.Base(fileOrPattern)
		s.Path = fileOrPattern
	} else {
		s.folder, _ = os.Getwd()
		s.fileBaseName = fileOrPattern
		s.Path = filepath.Join(s.folder, s.fileBaseName)
	}
	if strings.ContainsAny(s.fileBaseName, "*?") {
		s.hasWildcard = true
	}

	return s
}

// SetHasHeader sets whether the data contains header
func (q *CsvSource) SetHasHeader(hasHeader bool) *CsvSource {
	q.HasHeader = hasHeader
	return q
}

func (s *CsvSource) genShardInfos(f *flow.Flow) *flow.Dataset {
	return f.Source(func(writer io.Writer) error {
		if !s.hasWildcard && !filesystem.IsDir(s.Path) {
			util.WriteRow(writer, encodeShardInfo(&CsvShardInfo{
				FileName:  s.Path,
				HasHeader: s.HasHeader,
			}))
		} else {
			virtualFiles, err := filesystem.List(s.folder)
			if err != nil {
				return fmt.Errorf("Failed to list folder %s: %v", s.folder, err)
			}
			for _, vf := range virtualFiles {
				if !s.hasWildcard || s.match(vf.Location) {
					util.WriteRow(writer, encodeShardInfo(&CsvShardInfo{
						FileName:  vf.Location,
						HasHeader: s.HasHeader,
					}))
				}
			}
		}
		return nil
	})
}

func (s *CsvSource) match(fullPath string) bool {
	baseName := filepath.Base(fullPath)
	match, _ := filepath.Match(s.fileBaseName, baseName)
	return match
}
