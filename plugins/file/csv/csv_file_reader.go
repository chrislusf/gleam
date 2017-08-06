package csv

import (
	"io"

	"github.com/chrislusf/gleam/util"
)

type CsvFileReader struct {
	csvReader *Reader
}

func New(reader io.Reader) *CsvFileReader {
	return &CsvFileReader{
		csvReader: NewReader(reader),
	}
}

func (r *CsvFileReader) ReadHeader() (fieldNames []string, err error) {
	return r.csvReader.Read()
}
func (r *CsvFileReader) Read() (row *util.Row, err error) {
	var record []string
	var objects []interface{}
	record, err = r.csvReader.Read()
	if err != nil {
		return
	}
	for _, s := range record {
		objects = append(objects, s)
	}
	return util.NewRow(util.Now(), objects...), err
}
