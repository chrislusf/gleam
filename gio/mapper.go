package gio

import (
	"fmt"
	"io"
	"os"

	"github.com/chrislusf/gleam/util"
)

// Emit encode and write a row of data to os.Stdout
func Emit(anyObject ...interface{}) error {
	return util.WriteRow(os.Stdout, anyObject...)
}

func ProcessMapper(f Mapper) (err error) {
	for {
		row, err := util.ReadRow(os.Stdin)
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
