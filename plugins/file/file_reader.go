package file

import (
	"fmt"
	"io"

	"github.com/chrislusf/gleam/plugins/file/csv"
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

func (ds *FileShardInfo) NewReader(reader io.Reader) (FileReader, error) {
	switch ds.FileType {
	case "csv":
		return csv.New(reader), nil
	case "txt":
		return txt.New(reader), nil
	}
	return nil, fmt.Errorf("File type %s is not defined.", ds.FileType)
}
