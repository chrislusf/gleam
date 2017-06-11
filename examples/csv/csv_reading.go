package main

import (
	"os"

	"github.com/chrislusf/gleam/distributed"
	. "github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/gio"
	"github.com/chrislusf/gleam/plugins/csv"
)

func main() {

	gio.Init()

	f := New()

	a := f.Read(csv.New("a?.csv", 3).SetHasHeader(true)).Select(Field(1, 2, 3)).Hint(TotalSize(17))

	b := f.Read(csv.New("b*.csv", 3)).Select(Field(1, 4, 5)).Hint(PartitionSize(13))

	join := a.RightOuterJoin(b).Fprintf(os.Stdout, "%s : %s %s, %s %s\n")

	// join.Run(distributed.Planner())

	join.Run(distributed.Option())

}
