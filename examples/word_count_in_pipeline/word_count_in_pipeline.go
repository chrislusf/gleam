// word_count.go
package main

import (
	"fmt"

	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/gio"
	"github.com/chrislusf/gleam/gio/mapper"
	"github.com/chrislusf/gleam/util"
)

func main() {

	gio.Init()

	flow.New("word count by unix pipes").TextFile("/etc/passwd").
		Map("tokenize", mapper.Tokenize).
		Partition("partition", 2).
		Pipe("lowercase", "tr 'A-Z' 'a-z'").
		Pipe("write", "tee x.out").
		Pipe("sort", "sort").
		Pipe("uniq", "uniq -c").
		OutputRow(func(row *util.Row) error {

			fmt.Printf("%s\n", gio.ToString(row.K[0]))

			return nil
		}).Run()

}
