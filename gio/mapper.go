package gio

import (
	"fmt"
	"io"
	"os"

	"github.com/chrislusf/gleam/util"
)

var (
	rowTimeStamp int64
)

// Emit encode and write a row of data to os.Stdout
func Emit(anyObject ...interface{}) error {
	if rowTimeStamp == 0 {
		return util.NewRow(util.Now(), anyObject...).WriteTo(os.Stdout)
	}
	return util.NewRow(rowTimeStamp, anyObject...).WriteTo(os.Stdout)

}

// TsEmit encode and write a row of data to os.Stdout
// with ts in milliseconds epoch time
func TsEmit(ts int64, anyObject ...interface{}) error {
	return util.NewRow(ts, anyObject...).WriteTo(os.Stdout)
}

func ProcessMapper(f Mapper) error {
	for {
		row, err := util.ReadRow(os.Stdin)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("mapper input row error: %v", err)
		}
		var data []interface{}
		data = append(data, row.K...)
		data = append(data, row.V...)
		err = f(data)
		if err != nil {
			return fmt.Errorf("processing error: %v", err)
		}
	}
	return nil
}
