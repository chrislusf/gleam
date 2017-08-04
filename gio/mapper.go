package gio

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/chrislusf/gleam/util"
)

func (runner *gleamRunner) processMapper(ctx context.Context, f Mapper) (err error) {
	return runner.report(ctx, func() error {
		return runner.doProcessMapper(ctx, f)
	})
}

func (runner *gleamRunner) doProcessMapper(ctx context.Context, f Mapper) error {
	for {
		row, err := util.ReadRow(os.Stdin)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("mapper input row error: %v", err)
		}
		stat.Stats[0].InputCounter++

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
