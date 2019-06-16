package zipfile

import (
	"archive/zip"
	"bytes"
	"io"

	"github.com/chrislusf/gleam/util"
)

type FileReader struct {
	reader   *zip.ReadCloser
	Cursor   int
	NumFiles int
}

func New(filename string) *FileReader {
	r, _ := zip.OpenReader(filename)
	count := 0
	for _, f := range r.File {
		if f.FileInfo().IsDir() {
			continue
		}
		count++
	}

	return &FileReader{
		reader:   r,
		Cursor:   0,
		NumFiles: count,
	}
}

func (r *FileReader) ReadHeader() (fieldNames []string, err error) {
	return nil, nil
}

// Read will iterate through the zip file, it will treat each file
// in the zipfile as a row and return a byte array back to the caller
func (r *FileReader) Read() (row *util.Row, err error) {
	if r.Cursor >= r.NumFiles {
		return nil, io.EOF
	}
	f := r.reader.File[r.Cursor]
	fp, err := f.Open()
	if err != nil {
		return nil, err
	}
	object := new(bytes.Buffer)
	object.ReadFrom(fp)
	fp.Close()
	r.Cursor++
	return util.NewRow(util.Now(), object.Bytes()), nil
}
