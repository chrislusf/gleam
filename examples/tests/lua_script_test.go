package tests

import (
	"fmt"
	"os"
	"testing"

	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/util"
)

func TestCallingLuaScripts(t *testing.T) {

	data := [][]byte{
		[]byte("asdf"),
		[]byte("hlkgjh"),
		[]byte("truytyu"),
		[]byte("34weqrqw"),
		[]byte("asdfadfasaf"),
	}

	f := flow.New()

	outputChannel := f.Slice(data).Script("lua").Map(`
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
	`).LocalSort().Output()

	go flow.RunFlowContextSync(f)

	var outputData [][]byte
	for bytes := range outputChannel {
		outputData = append(outputData, bytes)
		util.PrintAsJSON(bytes, os.Stdout, true)
		fmt.Fprintln(os.Stdout)
	}

	if len(outputData) != 3 {
		t.Errorf("filter stops working!")
	}
}
