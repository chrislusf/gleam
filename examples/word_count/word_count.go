// word_count.go
package main

import (
	"fmt"
	"os"

	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/util"
)

func main() {
	shellFlow := flow.New()

	shellOutChan := shellFlow.TextFile("/etc/passwd").Pipe("grep -v '#'").Pipe(
		"awk {print}").Pipe("sort").Output()

	go flow.RunFlowContextSync(shellFlow)

	for bytes := range shellOutChan {
		util.PrintAsJSON(bytes, os.Stdout, true)
		fmt.Fprintln(os.Stdout)
	}

	luaFlow := flow.New()
	luaOutChan := luaFlow.TextFile("/etc/passwd").FlatMap(`
		function(line)
			return line:gmatch("%w+")
		end
	`).Pipe("sort").Pipe("uniq -c").Pipe("sort").Output()

	go flow.RunFlowContextSync(luaFlow)

	for bytes := range luaOutChan {
		util.PrintAsJSON(bytes, os.Stdout, true)
		fmt.Fprintln(os.Stdout)
	}
}
