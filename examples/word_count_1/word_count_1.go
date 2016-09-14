// word_count.go
package main

import (
	"fmt"

	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/util"
)

func main() {

	luaFlow := flow.New()
	outChan := luaFlow.TextFile("/etc/passwd").FlatMap(`
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
	`).Output()

	go flow.RunFlowContextSync(luaFlow)

	var word string
	var count int
	for bytes := range outChan {
		if err := util.DecodeRowTo(bytes, &word, &count); err != nil {
			fmt.Printf("decode error: %v", err)
			break
		}
		fmt.Printf("%s\t%d\n", word, count)
	}

}
