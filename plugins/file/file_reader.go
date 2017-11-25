package file

import (
	"fmt"

	"github.com/chrislusf/gleam/filesystem"
	"github.com/chrislusf/gleam/plugins/file/csv"
	"github.com/chrislusf/gleam/plugins/file/orc"
	"github.com/chrislusf/gleam/plugins/file/parquet"
	"github.com/chrislusf/gleam/plugins/file/tsv"
	"github.com/chrislusf/gleam/plugins/file/txt"
	"github.com/chrislusf/gleam/util"
)

type FileReader interface {
	Read() (row *util.Row, err error)
	ReadHeader() (fieldNames []string, err error)
}

func Csv(fileOrPattern string, partitionCount int) *FileSource {
	return newFileSource("csv", fileOrPattern, partitionCount)
}
func Txt(fileOrPattern string, partitionCount int) *FileSource {
	return newFileSource("txt", fileOrPattern, partitionCount)
}
func Tsv(fileOrPattern string, partitionCount int) *FileSource {
	return newFileSource("tsv", fileOrPattern, partitionCount)
}
func Orc(fileOrPattern string, partitionCount int) *FileSource {
	return newFileSource("orc", fileOrPattern, partitionCount)
}
func Parquet(fileOrPattern string, partitionCount int) *FileSource {
	return newFileSource("parquet", fileOrPattern, partitionCount)
}

func (ds *FileShardInfo) NewReader(vf filesystem.VirtualFile) (FileReader, error) {
	switch ds.FileType {
	case "csv":
		return csv.New(vf), nil
	case "txt":
		return txt.New(vf), nil
	case "tsv":
		return tsv.New(vf), nil
	case "orc":
		if reader, err := orc.New(vf); err == nil {
			return reader.Select(ds.Fields), nil
		} else {
			return nil, err
		}
	case "parquet":
		return parquet.New(vf, ds.FileName), nil
	}
	return nil, fmt.Errorf("File type %s is not defined.", ds.FileType)
}
