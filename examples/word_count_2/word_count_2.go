// word_count.go
package main

import (
	"os"

	"github.com/chrislusf/gleam/flow"
)

func main() {

	flow.New().TextFile("/etc/passwd").Partition(2).FlatMap(`
		function(line)
			return line:gmatch("%w+")
		end
	`).Map(`
		function(word)
			return word, 1
		end
	`).ReduceByKey(`
		function(x, y)
			return x + y
		end
	`).Fprintf(os.Stdout, "%s,%d\n").Run()

}
