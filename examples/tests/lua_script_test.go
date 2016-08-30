package tests

import (
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

	f := flow.New()

	outputChannel := f.Slice(data).Script("lua").Map(`
		function (line)
			print(line)
		end
	`).Script("lua",
		`function string.starts(String,Start)
		   return string.sub(String,1,string.len(Start))==Start
		end`,
	).Filter(`
		function (line)
			return not string.starts(line, 'asd')
		end
	`).Output()

	go flow.RunFlowContextSync(f)

	var outputData [][]byte
	for bytes := range outputChannel {
		println("lua:", string(bytes))
		outputData = append(outputData, bytes)
	}

	if len(outputData) != 3 {
		t.Errorf("filter stops working!")
	}
}
