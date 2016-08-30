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

	outputChannel := f.Slice(data).Script("sh").Map("grep -v asdf").Map("awk {print}").Output()
	// ch := f.TextFile("/etc/passwd").Script("sh").Map("sort").Output()

	go flow.RunFlowContextSync(f)

	var outputData [][]byte
	for bytes := range outputChannel {
		println(string(bytes))
		outputData = append(outputData, bytes)
	}

	if len(outputData) != 3 {
		t.Errorf("grep -v stops working!")
	}
}
