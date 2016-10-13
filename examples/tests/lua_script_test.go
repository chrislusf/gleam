package tests

import (
	"os"
	"testing"

	"github.com/chrislusf/gleam/flow"
)

func TestCallingLuaScripts(t *testing.T) {

	data := [][]byte{
		[]byte("asdf"),
		[]byte("hlkgjh"),
		[]byte("truytyu"),
		[]byte("34weqrqw"),
		[]byte("asdfadfasaf"),
	}

	flow.New().Bytes(data).Map(`
		function (line)
			return line
		end
	`).Script("lua",
		`function string.starts(String,Start)
		   return string.sub(String,1,string.len(Start))==Start
		end`,
	).Filter(`
		function (line)
			return not string.starts(line, 'asd')
		end
	`).Sort().Fprintf(os.Stdout, "lua > %s\n")

}
