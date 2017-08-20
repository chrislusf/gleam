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
)

func main() {

	gio.Init()

	f := New("join two csv files")

	a := f.Read(file.Csv("a?.csv", 3).SetHasHeader(true)).Select("select", Field(1, 2, 3)).Hint(TotalSize(17))

	b := f.Read(file.Csv("b*.csv", 3)).SelectKV("select", Field(1), Field(4, 5)).Hint(PartitionSize(13))

	join := a.RightOuterJoinByKey("join", b).Printlnf("%s : %s %s, %s %s")

	// join.Run(distributed.Planner())

	if *isDistributed {
		join.Run(distributed.Option())
	} else {
		join.Run()
	}

}
