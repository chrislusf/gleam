package txt

import (
	"bufio"
	"io"

	"github.com/chrislusf/gleam/util"
)

type TxtFileReader struct {
	scanner *bufio.Scanner
}

func New(reader io.Reader) *TxtFileReader {

	return &TxtFileReader{
		scanner: bufio.NewScanner(reader),
	}
}

func (r *TxtFileReader) ReadHeader() (fieldNames []string, err error) {
	return nil, nil
}
func (r *TxtFileReader) Read() (row *util.Row, err error) {
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
	return util.NewRow(util.Now(), string(data)), nil
}
