// word_count.go
package main

import (
	"os"

	"github.com/chrislusf/gleam/flow"
)

func main() {

	flow.New().
		TextFile("/etc/passwd").
		Pipe("tr 'A-Z' 'a-z'").
		Mapper("./go_mapper mapper1").
		Pipe("sort").
		Pipe("uniq -c").
		Fprintf(os.Stdout, "%s\n").Run()

}
