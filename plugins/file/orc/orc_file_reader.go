package orc

import (
	"github.com/chrislusf/gleam/util"
	"github.com/scritchley/orc"
	"io"
)

type OrcFileReader struct {
	reader *orc.Reader
	cursor *orc.Cursor
}

// TODO push down column projection
func New(reader orc.SizedReaderAt) (*OrcFileReader, error) {
	t, err := orc.NewReader(reader)
	if err != nil {
		return nil, err
	}
	c := t.Select(t.Schema().Columns()...)
	return &OrcFileReader{
		reader: t,
		cursor: c,
	}, nil
}

func (r *OrcFileReader) ReadHeader() (fieldNames []string, err error) {
	return nil, nil
}
func (r *OrcFileReader) Read() (row *util.Row, err error) {
	// Iterate over each row in the stripe.
	if r.cursor.Next() || r.cursor.Stripes() && r.cursor.Next() {
		if err := r.cursor.Err(); err != nil {
			return nil, err
		}
		return util.NewRow(util.Now(), r.cursor.Row()...), nil
	}
	return nil, io.EOF
}
