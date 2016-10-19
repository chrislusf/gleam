package main

import (
	"os"

	"github.com/chrislusf/gleam"
	"github.com/chrislusf/gleam/source/csv"
)

func main() {

	f := gleam.New(gleam.Local)
	a := f.Input(&csv.CsvInput{
		FileNames: []string{
			"/Users/chris/dev/gopath/src/github.com/chrislusf/gleam/examples/csv/sample0.csv",
			"/Users/chris/dev/gopath/src/github.com/chrislusf/gleam/examples/csv/sample1.csv",
			"/Users/chris/dev/gopath/src/github.com/chrislusf/gleam/examples/csv/sample2.csv",
		},
		HasHeader: true,
	}, 1).Select(1, 2, 3)

	b := f.Input(&csv.CsvInput{
		FileNames: []string{
			"/Users/chris/dev/gopath/src/github.com/chrislusf/gleam/examples/csv/sample0.csv",
		},
		HasHeader: true,
	}, 1).Select(1, 4, 5)

	a.Join(b).Fprintf(os.Stdout, "%s : %s %s, %s %s\n").Run()

}
