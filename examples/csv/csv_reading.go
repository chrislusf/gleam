package main

import (
	"os"

	"github.com/chrislusf/gleam"
	"github.com/chrislusf/gleam/source/csv"
)

func main() {

	f := gleam.New(gleam.DistributedPlanner)
	a := f.Input(csv.New(
		"/Users/chris/dev/gopath/src/github.com/chrislusf/gleam/examples/csv/sample1.csv",
		"/Users/chris/dev/gopath/src/github.com/chrislusf/gleam/examples/csv/sample2.csv",
	).SetHasHeader(true)).Select(1, 2, 3)

	b := f.Input(csv.New(
		"/Users/chris/dev/gopath/src/github.com/chrislusf/gleam/examples/csv/sample0.csv",
	)).Select(1, 4, 5)

	a.RightOuterJoin(b).Fprintf(os.Stdout, "%s : %s %s, %s %s\n").Run()

}
