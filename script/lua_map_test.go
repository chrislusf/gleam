package script

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"

	"github.com/chrislusf/gleam/util"
)

func TestLuaCommander(t *testing.T) {
	NewLuaScript()
}

func TestLuaMap(t *testing.T) {

	testScript(
		"test mapper",
		func(script Script) {
			script.Map(`function(x,y,z) return x+1, y.."yyy", not z end`)
		},
		func(inputWriter io.Writer) {
			util.WriteRow(inputWriter, 1999, "xxx", false)
		},
		func(outputReader io.Reader) {
			row, err := util.ReadRow(outputReader)
			if err != nil {
				fmt.Fprintf(os.Stderr, "read row error: %v", err)
				return
			}
			if !(row[0].(uint64) == 2000 && bytes.Equal(row[1].([]byte), []byte("xxxyyy")) && row[2].(bool) == true) {
				t.Errorf("failed map results: %+v", row)
			}

		},
	)
}

func TestLuaFilter(t *testing.T) {

	testScript(
		"test filter",
		func(script Script) {
			script.Filter(`
				function(x,y)
					return x > 0
				end
			`)
		},
		func(inputWriter io.Writer) {
			util.WriteRow(inputWriter, 1999, "x1999")
			util.WriteRow(inputWriter, -1, "x_1")
			util.WriteRow(inputWriter, 0, "x0")
			util.WriteRow(inputWriter, 1, "x1")
		},
		func(outputReader io.Reader) {
			row, _ := util.ReadRow(outputReader)
			if !(row[0].(uint64) == 1999 && bytes.Equal(row[1].([]byte), []byte("x1999"))) {
				t.Errorf("failed filter results: %+v", row)
			}

			row, _ = util.ReadRow(outputReader)
			if !(row[0].(uint64) == 1 && bytes.Equal(row[1].([]byte), []byte("x1"))) {
				t.Errorf("failed filter results: %+v", row)
			}

		},
	)
}

func TestLuaFlatMap(t *testing.T) {

	testScript(
		"test FlatMap",
		func(script Script) {
			script.FlatMap(`
				function(line)
					return line:gmatch("%w+")
				end
			`)
		},
		func(inputWriter io.Writer) {
			util.WriteRow(inputWriter, "x1 x2 x3")
		},
		func(outputReader io.Reader) {
			row, _ := util.ReadRow(outputReader)
			if !(bytes.Equal(row[0].([]byte), []byte("x1"))) {
				t.Errorf("failed FlatMap results: %+v", row)
			}
			row, _ = util.ReadRow(outputReader)
			if !(bytes.Equal(row[0].([]byte), []byte("x2"))) {
				t.Errorf("failed FlatMap results: %+v", row)
			}
			row, _ = util.ReadRow(outputReader)
			if !(bytes.Equal(row[0].([]byte), []byte("x3"))) {
				t.Errorf("failed FlatMap results: %+v", row)
			}
		},
	)
}

func TestLuaSelect(t *testing.T) {

	testScript(
		"test filter",
		func(script Script) {
			script.Select([]int{2, 1})
		},
		func(inputWriter io.Writer) {
			util.WriteRow(inputWriter, 1, "x1", 8)
			util.WriteRow(inputWriter, 2, "x2", 7)
		},
		func(outputReader io.Reader) {
			row, _ := util.ReadRow(outputReader)
			if !(row[1].(uint64) == 1 && bytes.Equal(row[0].([]byte), []byte("x1"))) {
				t.Errorf("failed select results: %+v", row)
			}

			row, _ = util.ReadRow(outputReader)
			if !(row[1].(uint64) == 2 && bytes.Equal(row[0].([]byte), []byte("x2"))) {
				t.Errorf("failed select results: %+v", row)
			}

		},
	)
}

func TestLuaLimit(t *testing.T) {

	testScript(
		"test Limit",
		func(script Script) {
			script.Limit(1)
		},
		func(inputWriter io.Writer) {
			util.WriteRow(inputWriter, 1, "x1", 8)
			util.WriteRow(inputWriter, 2, "x2", 7)
		},
		func(outputReader io.Reader) {
			row, _ := util.ReadRow(outputReader)
			row, _ = util.ReadRow(outputReader)
			if row != nil {
				t.Errorf("failed to take 1 row: %+v", row)
			}

		},
	)
}

func testScript(testName string, invokeLuaScriptFunc func(script Script),
	inputFunc func(inputWriter io.Writer),
	outputFunc func(outputReader io.Reader)) {
	var luaScript Script

	luaScript = NewLuaScript()
	luaScript.Init("")

	input, output := util.NewPiper(), util.NewPiper()

	invokeLuaScriptFunc(luaScript)

	var wg sync.WaitGroup
	wg.Add(1)
	go util.Execute(&wg, testName, luaScript.GetCommand().ToOsExecCommand(), input.Reader, output.Writer, false, false, true, os.Stderr)

	wg.Add(1)
	go func() {
		defer wg.Done()
		inputFunc(input.Writer)
		input.Writer.Close()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		outputFunc(output.Reader)
	}()

	wg.Wait()
}
