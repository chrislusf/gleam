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
	var commander Script

	commander = NewLuaScript()
	t.Log(commander.Name())
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

func TestLuaReduce(t *testing.T) {

	testScript(
		"test ReduceBy",
		func(script Script) {
			script.Reduce(`
				function(x, y)
					return x+y
				end
			`)
		},
		func(inputWriter io.Writer) {
			util.WriteRow(inputWriter, 100)
			util.WriteRow(inputWriter, 101)
			util.WriteRow(inputWriter, 101)
			util.WriteRow(inputWriter, 103)
		},
		func(outputReader io.Reader) {
			row, _ := util.ReadRow(outputReader)
			t.Logf("row1: %+v", row)
			if !(row[0].(uint64) == 405) {
				t.Errorf("failed ReduceBy results: %+v", row)
			}
		},
	)
}

func TestLuaReduceByMultipleValues(t *testing.T) {

	testScript(
		"test ReduceBy",
		func(script Script) {
			script.ReduceBy(`
				function(x, y, a, b)
					return x+a, y+b
				end
			`, []int{1})
		},
		func(inputWriter io.Writer) {
			util.WriteRow(inputWriter, "key1", 100, 133)
			util.WriteRow(inputWriter, "key2", 101, 3)
			util.WriteRow(inputWriter, "key2", 101, 4)
			util.WriteRow(inputWriter, "key3", 103, 138)
		},
		func(outputReader io.Reader) {
			row, _ := util.ReadRow(outputReader)
			t.Logf("row1: %+v", row)
			if !(row[1].(uint64) == 100 && row[2].(uint64) == 133) {
				t.Errorf("failed ReduceBy results: %+v", row)
			}
			row, _ = util.ReadRow(outputReader)
			t.Logf("row2: %+v", row)
			if !(row[1].(uint64) == 202 && row[2].(uint64) == 7) {
				t.Errorf("failed ReduceBy results: %+v", row)
			}
			row, _ = util.ReadRow(outputReader)
			t.Logf("row3: %+v", row)
			if !(row[1].(uint64) == 103 && row[2].(uint64) == 138) {
				t.Errorf("failed ReduceBy results: %+v", row)
			}
		},
	)
}

func TestLuaReduceBySingleValues(t *testing.T) {

	testScript(
		"test ReduceBy",
		func(script Script) {
			script.ReduceBy(`
				function(x, y)
					return x+y
				end
			`, []int{1, 2})
		},
		func(inputWriter io.Writer) {
			util.WriteRow(inputWriter, "key1", 100, 133)
			util.WriteRow(inputWriter, "key2", 101, 3)
			util.WriteRow(inputWriter, "key2", 101, 4)
			util.WriteRow(inputWriter, "key3", 103, 138)
		},
		func(outputReader io.Reader) {
			row, _ := util.ReadRow(outputReader)
			t.Logf("row1: %+v", row)
			if !(row[2].(uint64) == 133) {
				t.Errorf("failed ReduceBy results: %+v", row)
			}
			row, _ = util.ReadRow(outputReader)
			t.Logf("row2: %+v", row)
			if !(row[2].(uint64) == 7) {
				t.Errorf("failed ReduceBy results: %+v", row)
			}
			row, _ = util.ReadRow(outputReader)
			t.Logf("row3: %+v", row)
			if !(row[2].(uint64) == 138) {
				t.Errorf("failed ReduceBy results: %+v", row)
			}
		},
	)
}

func TestLuaGroupByMultipleValue(t *testing.T) {

	testScript(
		"test GroupBy",
		func(script Script) {
			script.GroupBy([]int{1, 2})
		},
		func(inputWriter io.Writer) {
			util.WriteRow(inputWriter, "key1", 100, 133, "r1")
			util.WriteRow(inputWriter, "key2", 101, 3, "r2")
			util.WriteRow(inputWriter, "key2", 101, 4, "r3")
			util.WriteRow(inputWriter, "key3", 103, 138, "r4")
		},
		func(outputReader io.Reader) {
			row, _ := util.ReadRow(outputReader)
			t.Logf("row1: %+v", row)
			if !(row[2].([]interface{})[0].(uint64) == 133) {
				t.Errorf("failed GroupBy results: %+v", row)
			}
			row, _ = util.ReadRow(outputReader)
			t.Logf("row2: %+v", row)
			if !(row[2].([]interface{})[0].(uint64) == 3) {
				t.Errorf("failed GroupBy results: %+v", row)
			}
			row, _ = util.ReadRow(outputReader)
			t.Logf("row3: %+v", row)
			if !(row[2].([]interface{})[0].(uint64) == 138) {
				t.Errorf("failed GroupBy results: %+v", row)
			}
		},
	)
}

func TestLuaGroupByZeroValue(t *testing.T) {

	testScript(
		"test GroupBy",
		func(script Script) {
			script.GroupBy([]int{1, 2})
		},
		func(inputWriter io.Writer) {
			util.WriteRow(inputWriter, "key1", 100)
			util.WriteRow(inputWriter, "key2", 101)
			util.WriteRow(inputWriter, "key2", 101)
			util.WriteRow(inputWriter, "key3", 103)
		},
		func(outputReader io.Reader) {
			row, _ := util.ReadRow(outputReader)
			t.Logf("row1: %+v", row)
			if !(row[2].(uint64) == 1) {
				t.Errorf("failed GroupBy results: %+v", row)
			}
			row, _ = util.ReadRow(outputReader)
			t.Logf("row2: %+v", row)
			if !(row[2].(uint64) == 2) {
				t.Errorf("failed GroupBy results: %+v", row)
			}
			row, _ = util.ReadRow(outputReader)
			t.Logf("row3: %+v", row)
			if !(row[2].(uint64) == 1) {
				t.Errorf("failed GroupBy results: %+v", row)
			}
		},
	)
}

func TestLuaGroupBySingleValues(t *testing.T) {

	testScript(
		"test GroupBy",
		func(script Script) {
			script.GroupBy([]int{1, 2})
		},
		func(inputWriter io.Writer) {
			util.WriteRow(inputWriter, "key1", 100, 133)
			util.WriteRow(inputWriter, "key2", 101, 3)
			util.WriteRow(inputWriter, "key2", 101, 4)
			util.WriteRow(inputWriter, "key3", 103, 138)
		},
		func(outputReader io.Reader) {
			row, _ := util.ReadRow(outputReader)
			t.Logf("row1: %+v", row)
			if !(row[2].([]interface{})[0].(uint64) == 133) {
				t.Errorf("failed GroupBy results: %+v", row)
			}
			row, _ = util.ReadRow(outputReader)
			t.Logf("row2: %+v", row)
			if !(row[2].([]interface{})[0].(uint64) == 3 && row[2].([]interface{})[1].(uint64) == 4) {
				t.Errorf("failed GroupBy results: %+v", row)
			}
			row, _ = util.ReadRow(outputReader)
			t.Logf("row3: %+v", row)
			if !(row[2].([]interface{})[0].(uint64) == 138) {
				t.Errorf("failed GroupBy results: %+v", row)
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
	go util.Execute(&wg, testName, luaScript.GetCommand().ToOsExecCommand(), input, output, false, false, true, os.Stderr)

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
