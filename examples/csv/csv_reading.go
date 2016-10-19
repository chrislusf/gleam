package main

import (
	"os"

	"github.com/chrislusf/gleam"
	"github.com/chrislusf/gleam/source/csv"
)

func main() {

	f := gleam.New(gleam.Distributed)
	f.Input(&csv.CsvInput{
		FileNames: []string{
			"/Users/chris/dev/gopath/src/github.com/chrislusf/gleam/examples/csv/sample0.csv",
			"/Users/chris/dev/gopath/src/github.com/chrislusf/gleam/examples/csv/sample0.csv",
		},
		HasHeader: true,
	}, 1).Select(1, 2, 3).Fprintf(os.Stdout, "%s : %s %s\n").Run()

}
