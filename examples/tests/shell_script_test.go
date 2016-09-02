package tests

import (
	"testing"

	"github.com/chrislusf/gleam/flow"
)

func TestCallingShellScripts(t *testing.T) {

	data := [][]byte{
		[]byte("asdf"),
		[]byte("hlkgjh"),
		[]byte("truytyu"),
		[]byte("34weqrqw"),
		[]byte("asdfadfasaf"),
	}

	f := flow.New()

	outputChannel := f.Slice(data).Pipe("grep -v asdf").Pipe("awk {print}").Output()
	// ch := f.TextFile("/etc/passwd").Script("sh").Map("sort").Output()

	go flow.RunFlowContextSync(f)

	var outputData [][]byte
	for bytes := range outputChannel {
		println("sh:", string(bytes))
		outputData = append(outputData, bytes)
	}

	if len(outputData) != 3 {
		t.Errorf("grep -v stops working!")
	}
}
