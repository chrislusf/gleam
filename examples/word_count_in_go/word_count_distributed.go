package main

import (
	"os"

	"github.com/chrislusf/gleam/distributed"
	"github.com/chrislusf/gleam/flow"
)

func main() {

	flow.New().
		TextFile("/etc/passwd").
		Pipe("tr 'A-Z' 'a-z'").
		Mapper("./go_mr tokenize").
		Pipe("sort").
		Mapper("./go_mr addOne").
		ReducerBy("./go_mr sum").
		Sort(flow.OrderBy(2, true)).
		Fprintf(os.Stdout, "%s %d\n").
		Run(
			distributed.Option().
				WithFile("./go_mr"),
		)

}
