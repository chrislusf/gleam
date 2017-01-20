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
	gio.RegisterMapper("tokenize", tokenize)
	gio.RegisterMapper("addOne", addOne)
	gio.RegisterReducer("sum", sum)
}

func main() {

	flag.Parse()
	gio.Init()

	f := flow.New().
		TextFile("/etc/passwd").
		Pipe("tr 'A-Z' 'a-z'").
		Mapper("tokenize").
		Pipe("sort").
		Mapper("addOne").
		ReducerBy("sum").
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
