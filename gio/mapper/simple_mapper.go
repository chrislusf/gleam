package mapper

import (
	"strings"

	"github.com/chrislusf/gleam/gio"
)

var (
	Tokenize  = gio.RegisterMapper(tokenize)
	AppendOne = gio.RegisterMapper(addOne)
)

func tokenize(row []interface{}) error {

	line := gio.ToString(row[0])

	for _, s := range strings.FieldsFunc(line, func(r rune) bool {
		return !('A' <= r && r <= 'Z' || 'a' <= r && r <= 'z' || '0' <= r && r <= '9')
	}) {
		gio.Emit(s)
	}

	return nil
}

func addOne(row []interface{}) error {
	row = append(row, 1)

	gio.Emit(row...)

	return nil
}
