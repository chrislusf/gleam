// word_count.go
package main

import (
	"os"

	"github.com/chrislusf/gleam/flow"
)

func main() {

	flow.New().TextFile("/etc/passwd").FlatMap(`
		function(line)
			if line then
				return line:gmatch("%w+")
			end
		end
	`).LocalSort().Map(`
		function(word)
			return word, 1
		end
	`).Reduce(`
		function(x, y)
			return x + y
		end
	`).Map(`
		function(k, v)
			return k .. " " .. v
		end
	`).Pipe("sort -n -k 2").SaveTextTo(os.Stdout)

}
