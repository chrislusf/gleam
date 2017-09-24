package main

import (
	"flag"

	"github.com/chrislusf/gleam/distributed"
	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/gio"
	"github.com/chrislusf/gleam/gio/mapper"
	"github.com/chrislusf/gleam/gio/reducer"
	"github.com/chrislusf/gleam/plugins/file"
)

var (
	isDistributed   = flag.Bool("distributed", false, "run in distributed or not")
	isDockerCluster = flag.Bool("onDocker", false, "run in docker cluster")
)

func main() {

	flag.Parse() // optional, since gio.Init() will call this also.
	gio.Init()   // If the command line invokes the mapper or reducer, execute it and exit.

	f := flow.New("top5 words in passwd").
		Read(file.Txt("/etc/passwd", 1)).
		Map("tokenize", mapper.Tokenize). // invoke the registered "tokenize" mapper function.
		Pipe("debugWithPipe", "tee debug.txt").
		Map("addOne", mapper.AppendOne).      // invoke the registered "addOne" mapper function.
		ReduceByKey("sum", reducer.SumInt64). // invoke the registered "sum" reducer function.
		Sort("sortBySum", flow.OrderBy(2, true)).
		Top("top5", 5, flow.OrderBy(2, false)).
		Printlnf("%s\t%d")

	if *isDistributed {
		f.Run(distributed.Option())
	} else if *isDockerCluster {
		f.Run(distributed.Option().SetMaster("master:45326"))
	} else {
		f.Run()
	}

}
