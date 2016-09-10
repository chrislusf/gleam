package tests

import (
	"os"
	"testing"

	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/util"
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

	f.Slice(data).Pipe("grep -v asdf").Pipe("awk {print}").SaveTextTo(os.Stdout)

}

func TestOutputObjects(t *testing.T) {

	data := [][]byte{
		[]byte("asdf"),
		[]byte("hlkgjh"),
		[]byte("truytyu"),
		[]byte("34weqrqw"),
		[]byte("asdfadfasaf"),
	}

	f := flow.New()

	outputChannel := f.Slice(data).Pipe("sort").Output()

	go flow.RunFlowContextSync(f)

	for bytes := range outputChannel {
		var line string
		util.DecodeRowTo(bytes, &line)
		println("sorted:", line)
	}
}
