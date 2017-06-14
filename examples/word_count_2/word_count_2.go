// word_count.go
package main

import (
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
	`).ReduceBy(`
		function(x, y)
			return x + y
		end
	`).Printlnf("%s,%d").Run()

}
