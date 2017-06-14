package main

import (
	"github.com/chrislusf/gleam/flow"
)

func main() {

	flow.New().TextFile("/etc/passwd").FlatMap(`
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
	`).Printlnf("%s\t%d").Run()
}
