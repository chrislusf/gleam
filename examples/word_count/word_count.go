// word_count.go
package main

import (
	"fmt"

	"github.com/chrislusf/gleam/flow"
)

func main() {
	fmt.Println("Hello World!")

	f := flow.New()
	// ch := f.TextFile("/etc/passwd").Output()
	ch := f.TextFile("/etc/passwd").Script("sh").Map("grep window").Map("awk {print}").Output()

	go flow.RunFlowContextSync(f)

	for bytes := range ch {
		println(string(bytes))
	}
}
