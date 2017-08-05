package file

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/chrislusf/gleam/filesystem"
	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/pb"
	"github.com/chrislusf/gleam/util"
)

type FileSource struct {
	folder         string
	fileBaseName   string
	hasWildcard    bool
	Path           string
	HasHeader      bool
	PartitionCount int
	FileType       string

	prefix string
}

// Generate generates data shard info,
// partitions them via round robin,
// and reads each shard on each executor
func (s *FileSource) Generate(f *flow.Flow) *flow.Dataset {
	return s.genShardInfos(f).RoundRobin(s.prefix, s.PartitionCount).Map(s.prefix+".Read", registeredMapperReadShard)
}

// SetHasHeader sets whether the data contains header
func (q *FileSource) SetHasHeader(hasHeader bool) *FileSource {
	q.HasHeader = hasHeader
	return q
}

// New creates a FileSource based on a file name.
// The base file name can have "*", "?" pattern denoting a list of file names.
func newFileSource(fileType, fileOrPattern string, partitionCount int) *FileSource {
	s := &FileSource{
		PartitionCount: partitionCount,
		FileType:       fileType,
		prefix:         "File",
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

func (s *FileSource) genShardInfos(f *flow.Flow) *flow.Dataset {
	return f.Source(s.prefix+"."+s.fileBaseName, func(writer io.Writer, stats *pb.InstructionStat) error {
		stats.InputCounter++
		if !s.hasWildcard && !filesystem.IsDir(s.Path) {
			stats.OutputCounter++
			util.NewRow(util.Now(), encodeShardInfo(&FileShardInfo{
				FileName:  s.Path,
				FileType:  s.FileType,
				HasHeader: s.HasHeader,
			})).WriteTo(writer)
		} else {
			virtualFiles, err := filesystem.List(s.folder)
			if err != nil {
				return fmt.Errorf("Failed to list folder %s: %v", s.folder, err)
			}
			for _, vf := range virtualFiles {
				if !s.hasWildcard || s.match(vf.Location) {
					stats.OutputCounter++
					util.NewRow(util.Now(), encodeShardInfo(&FileShardInfo{
						FileName:  vf.Location,
						FileType:  s.FileType,
						HasHeader: s.HasHeader,
					})).WriteTo(writer)
				}
			}
		}
		return nil
	})
}

func (s *FileSource) match(fullPath string) bool {
	baseName := filepath.Base(fullPath)
	match, _ := filepath.Match(s.fileBaseName, baseName)
	return match
}
