// word_count.go
package main

import (
	"github.com/chrislusf/gleam/flow"
)

func main() {
	shellFlow := flow.New()

	shellOutChan := shellFlow.TextFile("/etc/passwd").Script("sh").Map("grep -v window").Map("awk {print}").Output()
	// shellOutChan := shellFlow.TextFile("/etc/passwd").Script("sh").Map("sort").Output()

	go flow.RunFlowContextSync(shellFlow)

	for bytes := range shellOutChan {
		println("shell:", string(bytes))
	}

	luaFlow := flow.New()
	luaOutChan := luaFlow.TextFile("/etc/passwd").Script("lua").Map(`
		function(line)
			for w in line:gmatch("%w+") do print(w) end
		end
	`).Script("sh").Map("sort").Map("uniq -c").Map("sort").Output()

	go flow.RunFlowContextSync(luaFlow)

	for bytes := range luaOutChan {
		println(string(bytes))
	}

}
