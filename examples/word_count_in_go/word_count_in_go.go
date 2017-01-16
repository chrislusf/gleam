package main

import (
	"flag"
	"os"

	"github.com/chrislusf/gleam/distributed"
	"github.com/chrislusf/gleam/flow"
)

var (
	isDistributed = flag.Bool("distributed", false, "run in distributed or not")
)

func main() {

	flag.Parse()

	f := flow.New().
		TextFile("/etc/passwd").
		Pipe("tr 'A-Z' 'a-z'").
		Mapper("./mr/mr tokenize").
		Pipe("sort").
		Mapper("./mr/mr addOne").
		ReducerBy("./mr/mr sum").
		Sort(flow.OrderBy(2, true)).
		Fprintf(os.Stdout, "%s %d\n")

	if !*isDistributed {
		println("Running in standalone mode.")
		f.Run()
	} else {
		println("Running in distributed mode.")
		println("Adding the map reduce Go binaries.")
		f.Run(
			distributed.Option().
				WithFile("./mr/mr"),
		)
	}

}
