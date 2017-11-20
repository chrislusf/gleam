package parquet

import (
	"github.com/chrislusf/gleam/filesystem"
	"github.com/chrislusf/gleam/util"
	. "github.com/xitongsys/parquet-go/Common"
	. "github.com/xitongsys/parquet-go/ParquetHandler"
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
	_, err := self.reader.ReadAt([]byte{}, int64(offset))
	return int64(offset), err
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

	mp := parquetFileReader.pqHandler.SchemaHandler.MapIndex
	for key, _ := range mp {
		parquetFileReader.fieldNames = append(parquetFileReader.fieldNames, key)
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
		objects = append(objects, (*self.buffer)[fieldName].Values[self.cursor])
	}
	self.cursor++
	return util.NewRow(util.Now(), objects...), nil
}
