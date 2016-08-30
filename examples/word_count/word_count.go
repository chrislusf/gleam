// word_count.go
package main

import (
	"github.com/chrislusf/gleam/flow"
)

func main() {
	f := flow.New()

	ch := f.TextFile("/etc/passwd").Script("sh").Map("grep -v window").Map("awk {print}").Output()
	// ch := f.TextFile("/etc/passwd").Script("sh").Map("sort").Output()

	go flow.RunFlowContextSync(f)

	for bytes := range ch {
		println(string(bytes))
	}
}
