package main

import (
	"flag"
	"os"
	"strings"

	"github.com/chrislusf/gleam/distributed"
	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/gio"
)

var (
	isDistributed = flag.Bool("distributed", false, "run in distributed or not")
)

func init() {
	// Usually the functions are registered in init().
	// So functions registered in other packages can be shared.
	gio.RegisterMapper("tokenize", tokenize)
	gio.RegisterMapper("addOne", addOne)
	gio.RegisterReducer("sum", sum)
}

func main() {

	flag.Parse() // optional, since gio.Init() will call this also.
	gio.Init()   // If the command line invokes the mapper or reducer, execute it and exit.

	f := flow.New().
		TextFile("/etc/passwd").
		Pipe("tr 'A-Z' 'a-z'").
		Mapper("tokenize"). // invoke the registered "tokenize" mapper function.
		Pipe("sort").
		Mapper("addOne"). // invoke the registered "addOne" mapper function.
		ReducerBy("sum"). // invoke the registered "sum" reducer function.
		Sort(flow.OrderBy(2, true)).
		Fprintf(os.Stdout, "%s %d\n")

	if !*isDistributed {
		println("Running in standalone mode.")
		f.Run()
	} else {
		println("Running in distributed mode.")
		println("Adding the map reduce Go binaries.")
		f.Run(distributed.Option().WithDriverFile())
	}

}

func tokenize(row []interface{}) error {
	if len(row) == 0 {
		return nil
	}
	line := string(row[0].([]byte))

	if strings.HasPrefix(line, "#") {
		return nil
	}

	for _, s := range strings.Split(line, ":") {
		gio.Emit(s)
	}

	return nil
}

func addOne(row []interface{}) error {
	word := string(row[0].([]byte))

	gio.Emit(word, 1)

	return nil
}

func sum(x, y interface{}) (interface{}, error) {
	return x.(uint64) + y.(uint64), nil
}
