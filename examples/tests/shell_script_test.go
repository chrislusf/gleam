package tests

import (
	"os"
	"testing"

	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/util"
)

func TestCallingShellScripts(t *testing.T) {

	data := []string{
		"asdf",
		"hlkgjh",
		"truytyu",
		"34weqrqw",
		"asdfadfasaf",
	}

	f := flow.New()

	f.Strings(data).Pipe("grep -v asdf").Pipe("awk {print}").SaveTextTo(os.Stdout, "%s")

}

func TestOutputObjects(t *testing.T) {

	data := []string{
		"asdf",
		"hlkgjh",
		"truytyu",
		"34weqrqw",
		"asdfadfasaf",
	}

	f := flow.New()

	outputChannel := f.Strings(data).Pipe("sort").Output()

	go flow.RunFlowContextSync(f)

	for bytes := range outputChannel {
		var line string
		util.DecodeRowTo(bytes, &line)
		println("sorted:", line)
	}
}
