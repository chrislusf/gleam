package main

import (
	"strings"

	"github.com/chrislusf/gleam/gio"
)

func main() {

	gio.RegisterMapper("tokenize", mapperTokenize)
	gio.RegisterMapper("addOne", mapperAddOne)
	gio.RegisterReducer("sum", reducerSum)

	gio.RunMapperReducer()

}

func mapperTokenize(row []interface{}) error {
	if len(row) == 0 {
		return nil
	}
	line := string(row[0].([]byte))

	if strings.HasPrefix(line, "#") {
		return nil
	}

	for _, s := range strings.Split(line, ":") {
		gio.Emit(s)
	}

	return nil
}

func mapperAddOne(row []interface{}) error {
	word := string(row[0].([]byte))

	gio.Emit(word, 1)

	return nil
}

func reducerSum(x, y interface{}) (interface{}, error) {
	return x.(uint64) + y.(uint64), nil
}
