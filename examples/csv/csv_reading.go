package main

import (
	"os"

	"github.com/chrislusf/gleam/distributed"
	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/plugins/csv"
)

func main() {

	f := flow.New()
	a := f.ReadFile(csv.New("a?.csv").SetHasHeader(true)).Select(1, 2, 3)

	b := f.ReadFile(csv.New("b*.csv")).Select(1, 4, 5)

	a.RightOuterJoin(b).Fprintf(os.Stdout, "%s : %s %s, %s %s\n").Run(distributed.Option())

}
