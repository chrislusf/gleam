// word_count.go
package main

import (
	"os"

	"github.com/chrislusf/gleam"
)

func main() {

	gleam.New().TextFile("/etc/passwd").FlatMap(`
		function(line)
			return line:gmatch("%w+")
		end
	`).Pipe("tr 'A-Z' 'a-z'").Pipe("tee x.out").Pipe("sort").Pipe("uniq -c").Fprintf(os.Stdout, "%s\n").Run()
}
