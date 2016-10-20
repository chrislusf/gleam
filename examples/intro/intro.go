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
	`).Map(`
		function(word)
			return word, 1
		end
	`).ReduceBy(`
		function(x, y)
			return x + y
		end
	`).Fprintf(os.Stdout, "%s,%d\n").Run()
}
