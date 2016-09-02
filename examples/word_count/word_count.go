// word_count.go
package main

import (
	"github.com/chrislusf/gleam/flow"
)

func main() {
	shellFlow := flow.New()

	shellOutChan := shellFlow.TextFile("/etc/passwd").Pipe("grep -v '#'").Pipe(
		"awk {print}").Pipe("sort").Output()

	go flow.RunFlowContextSync(shellFlow)

	for bytes := range shellOutChan {
		println("shell:", string(bytes))
	}

	luaFlow := flow.New()
	luaOutChan := luaFlow.TextFile("/etc/passwd").FlatMap(`
		function(line)
			return line:gmatch("%w+")
		end
	`).Pipe("sort").Pipe("uniq -c").Pipe("sort").Output()

	go flow.RunFlowContextSync(luaFlow)

	for bytes := range luaOutChan {
		println(string(bytes))
	}
}
