package main

import (
	"strings"

	"github.com/chrislusf/gleam/gio"
)

func main() {

	gio.RegisterMapper("mapper1", mapper1)

	gio.Serve()

}

func mapper1(row []interface{}) error {
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
