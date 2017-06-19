package script

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/chrislusf/gleam/pb"
	"github.com/chrislusf/gleam/util"
)

func TestLuaCommander(t *testing.T) {
	NewLuajitScript()
}

func TestLuaMap(t *testing.T) {

	ts := time.Now().UnixNano() / int64(time.Millisecond)

	testLuaScript(
		"test mapper",
		func(script Script) {
			script.Map(`function(x,y,z) return x+1, y.."yyy", not z end`)
		},
		func(inputWriter io.Writer) {
			if err := util.WriteRow(inputWriter, ts, 1999, "xxx", false); err != nil {
				println("Failed to write row", err.Error())
			}
		},
		func(outputReader io.Reader) {
			ts1, row, err := util.ReadRow(outputReader)
			if err != nil {
				fmt.Fprintf(os.Stderr, "read map row error: %v", err)
				return
			}
			if ts1 != ts {
				t.Errorf("failed timestamp: %+v", row)
			}
			if !(row[0].(uint64) == 2000 && (row[1].(string) == "xxxyyy") && row[2].(bool) == true) {
				t.Errorf("failed map results: %+v", row)
			}

		},
	)
}

func TestLuaFilter(t *testing.T) {

	ts := time.Now().UnixNano() / int64(time.Millisecond)

	testLuaScript(
		"test filter",
		func(script Script) {
			script.Filter(`
				function(x,y)
					return x > 0
				end
			`)
		},
		func(inputWriter io.Writer) {
			util.WriteRow(inputWriter, ts, 1999, "x1999")
			util.WriteRow(inputWriter, ts, -1, "x_1")
			util.WriteRow(inputWriter, ts, 0, "x0")
			util.WriteRow(inputWriter, ts, 1, "x1")
		},
		func(outputReader io.Reader) {
			ts1, row, _ := util.ReadRow(outputReader)
			if ts1 != ts {
				t.Errorf("failed timestamp: %+v", row)
			}
			if !(row[0].(uint64) == 1999 && bytes.Equal(row[1].([]byte), []byte("x1999"))) {
				fmt.Printf("row: %+v\n", row)
				t.Errorf("failed filter results: %+v", row)
			}

			ts1, row, _ = util.ReadRow(outputReader)
			if !(row[0].(uint64) == 1 && bytes.Equal(row[1].([]byte), []byte("x1"))) {
				fmt.Printf("row: %+v\n", row)
				t.Errorf("failed filter results: %+v", row)
			}

		},
	)
}

func TestLuaFlatMap(t *testing.T) {

	ts := time.Now().UnixNano() / int64(time.Millisecond)

	testLuaScript(
		"test FlatMap",
		func(script Script) {
			script.FlatMap(`
				function(line)
					return line:gmatch("%w+")
				end
			`)
		},
		func(inputWriter io.Writer) {
			util.WriteRow(inputWriter, ts, "x1 x2 x3")
		},
		func(outputReader io.Reader) {
			ts1, row, _ := util.ReadRow(outputReader)
			if ts1 != ts {
				t.Errorf("failed timestamp: %+v", row)
			}
			if row[0].(string) != "x1" {
				t.Errorf("failed FlatMap results: %+v", row)
			}
			ts1, row, _ = util.ReadRow(outputReader)
			if row[0].(string) != "x2" {
				t.Errorf("failed FlatMap results: %+v", row)
			}
			ts1, row, _ = util.ReadRow(outputReader)
			if row[0].(string) != "x3" {
				t.Errorf("failed FlatMap results: %+v", row)
			}
		},
	)
}

func TestLuaMapWithNil(t *testing.T) {

	ts := time.Now().UnixNano() / int64(time.Millisecond)

	testLuaScript(
		"test mapper",
		func(script Script) {
			script.Map(`
			function(x, y, z)
                --log("received "..tostring(x)..":"..tostring(y)..":"..tostring(z))
				return x, y, z
			end`)
		},
		func(inputWriter io.Writer) {
			// The row we write has nil on index 1:
			util.WriteRow(inputWriter, ts, 8888, nil, "hello")
		},
		func(outputReader io.Reader) {
			ts1, row, err := util.ReadRow(outputReader)
			if err != nil {
				t.Errorf("read map nil row error: %v", err)
				return
			}
			if ts1 != ts {
				t.Errorf("failed timestamp: %+v", row)
			}
			if row[1] != nil {
				t.Errorf("Row no longer contains nil: %+v", row)
			}
			if row[2].(string) != "hello" {
				t.Errorf("Row no longer contains elements after nil: %+v", row[2])
			}
		},
	)
}

func TestLuaSelect(t *testing.T) {

	ts := time.Now().UnixNano() / int64(time.Millisecond)

	testLuaScript(
		"test filter",
		func(script Script) {
			script.Select([]int{2, 1})
		},
		func(inputWriter io.Writer) {
			util.WriteRow(inputWriter, ts, 1, "x1", 8)
			util.WriteRow(inputWriter, ts, 2, "x2", 7)
		},
		func(outputReader io.Reader) {
			ts1, row, _ := util.ReadRow(outputReader)
			if ts1 != ts {
				t.Errorf("failed timestamp: %+v", row)
			}
			if !(row[1].(uint64) == 1 && row[0].(string) == "x1") {
				t.Errorf("failed select results: %+v", row)
			}

			ts1, row, _ = util.ReadRow(outputReader)
			if !(row[1].(uint64) == 2 && row[0].(string) == "x2") {
				t.Errorf("failed select results: %+v", row)
			}

		},
	)
}

func TestLuaLimit(t *testing.T) {

	ts := time.Now().UnixNano() / int64(time.Millisecond)

	testLuaScript(
		"test Limit",
		func(script Script) {
			script.Limit(1, 1)
		},
		func(inputWriter io.Writer) {
			util.WriteRow(inputWriter, ts, 1, "x1", 8)
			util.WriteRow(inputWriter, ts, 2, "x2", 7)
			util.WriteRow(inputWriter, ts, 3, "x3", 6)
			util.WriteRow(inputWriter, ts, 4, "x4", 21)
			util.WriteRow(inputWriter, ts, 5, "x5", 22)
		},
		func(outputReader io.Reader) {
			// read first row
			ts1, row, _ := util.ReadRow(outputReader)
			fmt.Printf("row: %+v\n", row)
			if ts1 != ts {
				t.Errorf("failed timestamp: %+v", row)
			}
			if !(row[0].(uint64) == 2 && row[1].(string) == "x2") {
				t.Errorf("failed select results: %+v", row)
			}

			// read second row
			ts1, row, _ = util.ReadRow(outputReader)
			if row != nil {
				t.Errorf("failed to take 1 row: %+v", row)
			}
		},
	)
}

func testLuaScript(testName string, invokeLuaScriptFunc func(script Script),
	inputFunc func(inputWriter io.Writer),
	outputFunc func(outputReader io.Reader)) {

	var luaScript Script

	luaScript = NewLuajitScript()
	luaScript.Init("")

	testScript(testName, luaScript, invokeLuaScriptFunc, inputFunc, outputFunc)
}

func testScript(testName string, script Script, invokeScriptFunc func(script Script),
	inputFunc func(inputWriter io.Writer),
	outputFunc func(outputReader io.Reader)) {

	input, output := util.NewPiper(), util.NewPiper()

	invokeScriptFunc(script)

	var wg sync.WaitGroup
	wg.Add(1)
	go util.Execute(context.Background(), &wg, &pb.InstructionStat{}, testName, script.GetCommand().ToOsExecCommand(), input.Reader, output.Writer, false, false, true, os.Stderr)

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
