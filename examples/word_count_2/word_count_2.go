// word_count.go
package main

import (
	"os"

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
	`).Reduce(`
		function(x, y)
			return x + y
		end
	`).SaveTextTo(os.Stdout, "%s,%d")

}
