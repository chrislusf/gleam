package main

import (
	"os"

	"github.com/chrislusf/gleam/distributed"
	. "github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/plugins/csv"
)

func main() {

	f := New()

	a := f.ReadFile(csv.New("a?.csv").SetHasHeader(true)).Select(Field(1, 2, 3))

	b := f.ReadFile(csv.New("b*.csv")).Select(Field(1, 4, 5))

	join := a.RightOuterJoin(b).Fprintf(os.Stdout, "%s : %s %s, %s %s\n")

	// join.Run(distributed.Planner())

	join.Run(distributed.Option())

}
