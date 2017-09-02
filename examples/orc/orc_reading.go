package main

import (
	"flag"

	"github.com/chrislusf/gleam/distributed"
	. "github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/gio"
	"github.com/chrislusf/gleam/plugins/file"
)

var (
	isDistributed = flag.Bool("distributed", false, "run in distributed mode")
	fileNames     = flag.String("files", "T*.orc", "the list of orc files")

	absId = gio.RegisterMapper(absolute)
)

func init() {

}

func main() {

	gio.Init()

	f := New("reading orc files").
		Read(file.Orc(*fileNames, 3).
			Select("string1", "int1")). // push down the field selection to orc file
		Map("adjust integer", absId).
		Select("reverse", Field(2, 1)).
		Printlnf("%v : %v")

	if *isDistributed {
		f.Run(distributed.Option())
	} else {
		f.Run()
	}

}

func absolute(row []interface{}) error {
	a, b := gio.ToString(row[0]), gio.ToInt64(row[1])
	if b < 0 {
		b = -b
	}
	gio.Emit(a, b)
	return nil
}
