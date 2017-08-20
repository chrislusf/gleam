package orc

import (
	"io"

	"github.com/chrislusf/gleam/util"
	"github.com/scritchley/orc"
)

type OrcFileReader struct {
	reader     *orc.Reader
	cursor     *orc.Cursor
	fieldNames []string
}

// TODO predicate pushdown
func New(reader orc.SizedReaderAt) (*OrcFileReader, error) {
	t, err := orc.NewReader(reader)
	if err != nil {
		return nil, err
	}
	return &OrcFileReader{
		reader:     t,
		fieldNames: t.Schema().Columns(),
	}, nil
}
func (r *OrcFileReader) Select(fields []string) *OrcFileReader {
	if fields != nil {
		r.fieldNames = fields
	}
	return r
}

func (r *OrcFileReader) ReadHeader() (fieldNames []string, err error) {
	return r.fieldNames, nil
}
func (r *OrcFileReader) Read() (row *util.Row, err error) {
	if r.cursor == nil {
		r.cursor = r.reader.Select(r.fieldNames...)
	}
	// Iterate over each row in the stripe.
	if r.cursor.Next() || r.cursor.Stripes() && r.cursor.Next() {
		if err := r.cursor.Err(); err != nil {
			return nil, err
		}
		return util.NewRow(util.Now(), r.cursor.Row()...), nil
	}
	return nil, io.EOF
}
