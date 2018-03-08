package main

import (
	"github.com/chrislusf/gleam/distributed"
	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/gio"
	"github.com/chrislusf/gleam/gio/mapper"
	"github.com/chrislusf/gleam/plugins/file"
)

func main() {
	gio.Init()

	flow.New("top").
		Read(file.Txt("/Users/francesc/src/github.com/campoy/justforfunc/*", 1)).
		Map("tokenize", mapper.Tokenize).
		// Map("addOne", mapper.AppendOne).
		// ReduceByKey("sum", reducer.SumInt64).
		// Sort("sort by count", flow.OrderBy(2, false)).
		// Top("top 5", 5, flow.OrderBy(2, false)).
		// Fprintf(os.Stdout, "%s\n").
		Run(distributed.Option())
}
