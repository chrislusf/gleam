package flow_test

import (
	"fmt"
	"io"
	"testing"

	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/gio"
	"github.com/chrislusf/gleam/pb"
	"github.com/chrislusf/gleam/util"
	"github.com/stretchr/testify/assert"
)

func toRows(data [][]interface{}) func(io.Writer, *pb.InstructionStat) error {
	return func(outWriter io.Writer, stat *pb.InstructionStat) error {
		for _, values := range data {
			if err := util.NewRow(util.Now(), values...).WriteTo(outWriter); err != nil {
				return err
			}
			stat.OutputCounter++
		}
		return nil
	}
}

func addX(row []interface{}, args ...interface{}) error {
	row = []interface{}{row[0], args[0].(float64)}
	gio.Emit(row...)
	return nil
}

var addXId = gio.RegisterMapper(addX)

func TestMapper(t *testing.T) {
	gio.Init()

	// function to be used in Output. Stores the result in `result`
	var result []interface{}
	collect := func(r io.Reader) error {
		return util.ProcessMessage(r, func(encodedBytes []byte) error {
			row, _ := util.DecodeRow(encodedBytes)
			result = append(result, row)
			return nil
		})
	}

	data := make([][]interface{}, 2)
	for i, _ := range data {
		data[i] = make([]interface{}, 2)
		data[i] = []interface{}{i, i}
	}

	var arg1 int64 = 10

	flow.New("").Source("a", toRows(data)).Map("add10", addXId, arg1).Output(collect).Run()

	fmt.Println(result)

	assert.Equal(t, []interface{}{int64(0)}, result[0].(*util.Row).K)
	assert.Equal(t, []interface{}{arg1}, result[0].(*util.Row).V)

	assert.Equal(t, []interface{}{int64(1)}, result[1].(*util.Row).K)
	assert.Equal(t, []interface{}{arg1}, result[1].(*util.Row).V)
}
