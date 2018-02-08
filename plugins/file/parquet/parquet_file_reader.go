package parquet

import (
	"github.com/chrislusf/gleam/filesystem"
	"github.com/chrislusf/gleam/util"
	. "github.com/xitongsys/parquet-go/ParquetFile"
	. "github.com/xitongsys/parquet-go/ParquetReader"
	. "github.com/xitongsys/parquet-go/ParquetType"
	"io"
)

type PqFile struct {
	FileName string
	VF       filesystem.VirtualFile
}

func (self *PqFile) Create(name string) (ParquetFile, error) {
	return nil, nil
}

func (self *PqFile) Open(name string) (ParquetFile, error) {
	if name == "" {
		name = self.FileName
	}
	vf, err := filesystem.Open(name)
	if err != nil {
		return nil, err
	}
	res := &PqFile{
		VF:       vf,
		FileName: name,
	}
	return res, nil
}
func (self *PqFile) Seek(offset int, pos int) (int64, error) {
	return self.VF.Seek(int64(offset), pos)
}
func (self *PqFile) Read(b []byte) (n int, err error) {
	return self.VF.Read(b)
}
func (self *PqFile) Write(b []byte) (n int, err error) {
	return 0, nil
}
func (self *PqFile) Close() {}

type ParquetFileReader struct {
	pqReader *ParquetReader
	NumRows  int
	Cursor   int
}

func New(reader filesystem.VirtualFile, fileName string) *ParquetFileReader {
	parquetFileReader := new(ParquetFileReader)
	var pqFile ParquetFile = &PqFile{}
	pqFile, _ = pqFile.Open(fileName)
	parquetFileReader.pqReader, _ = NewParquetColumnReader(pqFile, 1)
	parquetFileReader.NumRows = int(parquetFileReader.pqReader.GetNumRows())
	return parquetFileReader
}

func (self *ParquetFileReader) ReadHeader() (fieldNames []string, err error) {
	return self.pqReader.SchemaHandler.ValueColumns, nil
}

func (self *ParquetFileReader) Read() (row *util.Row, err error) {
	if self.Cursor >= self.NumRows {
		return nil, io.EOF
	}
	objects := make([]interface{}, 0)
	for _, fieldName := range self.pqReader.SchemaHandler.ValueColumns {
		schemaIndex := self.pqReader.SchemaHandler.MapIndex[fieldName]
		values, _, _ := self.pqReader.ReadColumnByPath(fieldName, 1)
		objects = append(objects, ParquetTypeToGoType(values[0],
			self.pqReader.SchemaHandler.SchemaElements[schemaIndex].Type,
			self.pqReader.SchemaHandler.SchemaElements[schemaIndex].ConvertedType,
		))
	}
	self.Cursor++
	return util.NewRow(util.Now(), objects...), nil
}
