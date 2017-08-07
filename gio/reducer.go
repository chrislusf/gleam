package gio

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/chrislusf/gleam/util"
)

func (runner *gleamRunner) processReducer(ctx context.Context, f Reducer, keyPositions []int) (err error) {
	return runner.report(ctx, func() error {
		if len(keyPositions) == 1 && keyPositions[0] == 0 {
			return runner.doProcessReducer(f)
		}
		return runner.doProcessReducerByKeys(f, keyPositions)
	})
}

func (runner *gleamRunner) doProcessReducer(f Reducer) (err error) {
	// get the first row
	row, err := util.ReadRow(os.Stdin)
	if err != nil {
		if err == io.EOF {
			return nil
		}
		return fmt.Errorf("reducer input row error: %v", err)
	}
	stat.Stats[0].InputCounter++

	lastTs := row.T
	lastKeys := row.K

	for {
		row, err = util.ReadRow(os.Stdin)
		if err != nil {
			if err != io.EOF {
				fmt.Fprintf(os.Stderr, "join read row error: %v", err)
			}
			break
		}
		stat.Stats[0].InputCounter++

		keys := row.K
		lastKeys, err = reduce(f, lastKeys, keys)
		if row.T > lastTs {
			lastTs = row.T
		}
	}

	// fmt.Fprintf(os.Stderr, "lastKeys:%v\n", lastKeys)

	TsEmit(lastTs, lastKeys...)

	return nil
}

func (runner *gleamRunner) doProcessReducerByKeys(f Reducer, keyPositions []int) (err error) {

	// get the first row
	row, err := util.ReadRow(os.Stdin)
	if err != nil {
		if err == io.EOF {
			return nil
		}
		return fmt.Errorf("reducer input row error: %v", err)
	}
	stat.Stats[0].InputCounter++

	lastTs := row.T
	row.UseKeys(keyPositions)
	lastKeys, lastValues := row.K, row.V

	for {
		row, err = util.ReadRow(os.Stdin)
		if err != nil {
			if err != io.EOF {
				fmt.Fprintf(os.Stderr, "join read row error: %v", err)
			}
			break
		}
		stat.Stats[0].InputCounter++

		row.UseKeys(keyPositions)
		keys, values := row.K, row.V
		x := util.Compare(lastKeys, keys)
		if x == 0 {
			lastValues, err = reduce(f, lastValues, values)
		} else {
			TsEmitKV(lastTs, lastKeys, lastValues)
			lastKeys, lastValues = keys, values
		}
		if row.T > lastTs {
			lastTs = row.T
		}
	}
	TsEmitKV(lastTs, lastKeys, lastValues)

	return nil
}

func reduce(f Reducer, x, y []interface{}) ([]interface{}, error) {
	if len(x) == 1 && len(y) == 1 {
		z, err := f(x[0], y[0])
		if err != nil {
			return nil, err
		}
		return []interface{}{z}, nil
	}
	z, err := f(x, y)
	if err != nil {
		return nil, err
	}
	return z.([]interface{}), nil
}
