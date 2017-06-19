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
		return util.WriteRow(os.Stdout, util.Now(), anyObject...)
	}
	return util.WriteRow(os.Stdout, rowTimeStamp, anyObject...)

}

// TsEmit encode and write a row of data to os.Stdout
// with ts in milliseconds epoch time
func TsEmit(ts int64, anyObject ...interface{}) error {
	return util.WriteRow(os.Stdout, ts, anyObject...)
}

func ProcessMapper(f Mapper) (err error) {
	var row []interface{}
	for {
		rowTimeStamp, row, err = util.ReadRow(os.Stdin)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("input row error: %v", err)
		}
		err = f(row)
		if err != nil {
			return fmt.Errorf("processing error: %v", err)
		}
	}
	return nil
}
