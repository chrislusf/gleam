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
	`).Pipe("sort").Pipe("uniq -c").SaveTextTo(os.Stdout, "%s")
}
