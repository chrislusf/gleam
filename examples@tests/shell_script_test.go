package tests

import (
	"os"
	"testing"

	"github.com/chrislusf/gleam/flow"
)

func TestCallingShellScripts(t *testing.T) {

	data := []string{
		"asdf",
		"hlkgjh",
		"truytyu",
		"34weqrqw",
		"asdfadfasaf",
	}

	flow.New("callShell").Strings(data).
		Pipe("filter", "grep -v asdf").
		Pipe("print", "awk {print}").
		Fprintf(os.Stdout, "calling shell> %s\n")

}

func TestOutputObjects(t *testing.T) {

	data := []string{
		"asdf",
		"hlkgjh",
		"truytyu",
		"34weqrqw",
		"asdfadfasaf",
	}

	flow.New("stringOutput").Strings(data).
		Pipe("sort", "sort").
		Fprintf(os.Stdout, "shell> %s\n")

}
