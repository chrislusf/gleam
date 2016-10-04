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

	flow.New().Strings(data).Pipe("grep -v asdf").Pipe("awk {print}").Fprintf(os.Stdout, "%s\n")

}

func TestOutputObjects(t *testing.T) {

	data := []string{
		"asdf",
		"hlkgjh",
		"truytyu",
		"34weqrqw",
		"asdfadfasaf",
	}

	flow.New().Strings(data).Pipe("sort").Fprintf(os.Stdout, "shell> %s\n")

}
