package parquet

import (
	"github.com/chrislusf/gleam/util"
	. "github.com/xitongsys/parquet-go/Common"
	. "github.com/xitongsys/parquet-go/ParquetHandler"
	. "github.com/xitongsys/parquet-go/ParquetType"
	"io"
)

type PqFile struct {
	reader io.Reader
}

func (self *PqFile) Create(name string) (ParquetFile, error) {
	return nil, nil
}

func (self *PqFile) Open(name string) (ParquetFile, error) {
	return self
}
func (self *PqFile) Seek(offset int, pos int) (int64, error) {
	return self.File.Seek(int64(offset), pos)
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

func New(reader io.Reader) *ParquetFileReader {
	parquetReader := new(ParquetFileReader)
	pqFile := &PqFile{
		reader: reader,
	}
	parquetFileReader.pqHandler = NewParquetHandler()
	parquetFileReader.rowGroupNum = parquetReader.pqHandler.ReadInit(pqFile, 1)

	mp := parquetReader.pqHandler.SchemaHandler.MapIndex
	for key, _ := range mp {
		parquetFileReader.fieldNames = append(self.fieldNames, key)
	}

	return parquetFileReader
}

func (self *ParquetFileReader) ReadHeader(fieldNames []string, err error) {
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
		objects = append(objects, self.buffer[fieldName].Values[self.cursor])
	}
	self.cursor++
	return util.NewRow(util.Now(), objects...), nil
}
