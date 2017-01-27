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
	MapperTokenizer = gio.RegisterMapper(tokenize)
	MapperAddOne    = gio.RegisterMapper(addOne)
	ReducerSum      = gio.RegisterReducer(sum)

	isDistributed = flag.Bool("distributed", false, "run in distributed or not")
)

func main() {

	flag.Parse() // optional, since gio.Init() will call this also.
	gio.Init()   // If the command line invokes the mapper or reducer, execute it and exit.

	f := flow.New().TextFile("/etc/passwd").
		Mapper(MapperTokenizer). // invoke the registered "tokenize" mapper function.
		Mapper(MapperAddOne).    // invoke the registered "addOne" mapper function.
		ReducerBy(ReducerSum).   // invoke the registered "sum" reducer function.
		Sort(flow.OrderBy(2, true)).
		Fprintf(os.Stdout, "%s\t%d\n")

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

	line := string(row[0].([]byte))

	for _, s := range strings.FieldsFunc(line, func(r rune) bool {
		return !('A' <= r && r <= 'Z' || 'a' <= r && r <= 'z' || '0' <= r && r <= '9')
	}) {
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
