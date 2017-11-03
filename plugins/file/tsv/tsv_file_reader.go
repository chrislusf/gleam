package tsv

import (
	"bufio"
	"io"
	"strings"

	"github.com/chrislusf/gleam/util"
)

type TsvFileReader struct {
	scanner *bufio.Scanner
}

func New(reader io.Reader) *TsvFileReader {
	return &TsvFileReader{
		scanner: bufio.NewScanner(reader),
	}
}

func (r *TsvFileReader) ReadHeader() (fieldNames []string, err error) {
	return r.readOneLine()
}
func (r *TsvFileReader) Read() (row *util.Row, err error) {
	var values []string
	values, err = r.readOneLine()
        if err != nil {
		return nil, err
	}
	var data []interface{}
	for _, v := range values {
		data = append(data, v)
	}
	return util.NewRow(util.Now(), data...), nil
}

func (r *TsvFileReader) readOneLine() (values []string, err error) {
	var data []byte
	if r.scanner.Scan() {
		data = r.scanner.Bytes()
	} else {
		err = r.scanner.Err()
		if err == nil {
			err = io.EOF
		}
		return nil, err
	}
	return strings.Split(string(data), "\t"), nil
}
