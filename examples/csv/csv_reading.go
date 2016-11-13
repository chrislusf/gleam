package main

import (
	"os"

	"github.com/chrislusf/gleam/adapter"
	"github.com/chrislusf/gleam/distributed"
	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/plugins/csv"
)

func main() {

	adapter.RegisterConnection("csv", "csv")

	aCsv := csv.New("a?.csv")
	aCsv.HasHeader = true

	f := flow.New()
	a := f.Query("csv", aCsv).Select(1, 2, 3)

	b := f.Query("csv", csv.New("b*.csv")).Select(1, 4, 5)

	a.RightOuterJoin(b).Fprintf(os.Stdout, "%s : %s %s, %s %s\n").Run(distributed.Option())

}
