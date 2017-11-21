package parquet

import (
	"github.com/chrislusf/gleam/filesystem"
	"github.com/chrislusf/gleam/util"
	. "github.com/xitongsys/parquet-go/Common"
	. "github.com/xitongsys/parquet-go/ParquetHandler"
	. "github.com/xitongsys/parquet-go/ParquetType"
	"io"
)

type PqFile struct {
	reader filesystem.VirtualFile
}

func (self *PqFile) Create(name string) (ParquetFile, error) {
	return nil, nil
}

func (self *PqFile) Open(name string) (ParquetFile, error) {
	return self, nil
}
func (self *PqFile) Seek(offset int, pos int) (int64, error) {
	return self.reader.Seek(int64(offset), pos)
}
func (self *PqFile) Read(b []byte) (n int, err error) {
	return self.reader.Read(b)
}
func (self *PqFile) Write(b []byte) (n int, err error) {
	return 0, nil
}
func (self *PqFile) Close() {}

type ParquetFileReader struct {
	pqHandler   *ParquetHandler
	rowGroupNum int
	buffer      *map[string]*Table
	cursor      int
	size        int

	fieldNames []string
}

func New(reader filesystem.VirtualFile) *ParquetFileReader {
	parquetFileReader := new(ParquetFileReader)
	pqFile := &PqFile{
		reader: reader,
	}

	parquetFileReader.pqHandler = NewParquetHandler()
	parquetFileReader.rowGroupNum = parquetFileReader.pqHandler.ReadInit(pqFile, 1)

	sH := parquetFileReader.pqHandler.SchemaHandler
	for i := 0; i < len(sH.SchemaElements); i++ {
		schema := sH.SchemaElements[i]
		if schema.GetNumChildren() == 0 {
			pathStr := sH.IndexMap[int32(i)]
			parquetFileReader.fieldNames = append(parquetFileReader.fieldNames, pathStr)
		}
	}

	return parquetFileReader
}

func (self *ParquetFileReader) ReadHeader() (fieldNames []string, err error) {
	return self.fieldNames, nil
}

func (self *ParquetFileReader) Read() (row *util.Row, err error) {
	var objects []interface{}
	if self.cursor >= self.size && self.rowGroupNum > 0 {
		self.buffer, self.size = self.pqHandler.ReadOneRowGroup()
		self.rowGroupNum--
		self.cursor = 0
	} else if self.cursor >= self.size {
		return nil, io.EOF
	}

	for _, fieldName := range self.fieldNames {
		value := (*self.buffer)[fieldName].Values[self.cursor]
		objects = append(objects, ParquetTypeToGoType(value))
	}
	self.cursor++
	return util.NewRow(util.Now(), objects...), nil
}
