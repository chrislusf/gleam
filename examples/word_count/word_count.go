// word_count.go
package main

import (
	"fmt"
	"os"

	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/util"
)

func main() {

	luaFlow := flow.New()
	luaOutChan := luaFlow.TextFile("/etc/passwd").FlatMap(`
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
			return k .. "\t" .. v
		end
	`).Pipe("sort -n -k 2").Output()

	go flow.RunFlowContextSync(luaFlow)

	for bytes := range luaOutChan {
		var line string
		if err := util.DecodeRowTo(bytes, &line); err != nil {
			fmt.Printf("decode error: %v", err)
			break
		}
		fmt.Fprintf(os.Stdout, "output>%s\n", line)
	}

}
