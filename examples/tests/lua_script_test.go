package tests

import (
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

	outputCounter := 0
	for bytes := range outputChannel {
		outputCounter++
		var line []byte
		util.DecodeRowTo(bytes, &line)
		println("lua > ", string(line))
	}

	if outputCounter != 3 {
		t.Errorf("filter stops working!")
	}
}
