// word_count.go
package main

import (
	"github.com/chrislusf/gleam/flow"
)

func main() {
	f := flow.New()

	ch := f.TextFile("/etc/passwd").Script("sh").Map("grep window").Map("awk {print}").Output()

	go flow.RunFlowContextSync(f)

	for bytes := range ch {
		println("line:", string(bytes))
	}
}
