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
	numFilesAndDirs := len(r.File)
	return &FileReader{
		reader:   r,
		Cursor:   0,
		NumFiles: numFilesAndDirs,
	}
}

func (r *FileReader) ReadHeader() (fieldNames []string, err error) {
	return []string{"file_name", "file_content"}, nil
}

// Read will iterate through the zip file, it will treat each file
// in the zipfile as a row and return back to the caller, where the
// key is file or directory name and the value is the bytes of the
// file from the input zip file
func (r *FileReader) Read() (row *util.Row, err error) {
	if r.Cursor >= r.NumFiles {
		return nil, io.EOF
	}
	f := r.reader.File[r.Cursor]
	fp, err := f.Open()
	if err != nil {
		return nil, err
	}
	defer fp.Close()
	data := new(bytes.Buffer)
	data.ReadFrom(fp)
	row = util.NewRow(util.Now())
	if !f.FileInfo().IsDir() {
		row.AppendKey(f.Name).AppendValue(data.Bytes())
	} else {
		row.AppendKey(f.Name).AppendValue([]byte{})
	}
	r.Cursor++
	return row, nil
}
