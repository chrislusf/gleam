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
	showSchema    = flag.Bool("showSchema", false, "print out the columns")
	fileNames     = flag.String("files", "T*.orc", "the list of orc files")
)

func main() {

	gio.Init()

	f := New("reading orc files").
		Read(file.Orc(*fileNames, 3)).
		Select("select", Field(1, 2)).
		Printlnf("%v : %v")

	if *isDistributed {
		f.Run(distributed.Option())
	} else {
		f.Run()
	}

}
