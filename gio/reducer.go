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
		return runner.doProcessReducer(f, keyPositions)
	})
}

func (runner *gleamRunner) doProcessReducer(f Reducer, keyPositions []int) (err error) {

	keyFields := make([]bool, len(keyPositions))
	for _, keyPosition := range keyPositions {
		// change from 1-base to 0-base
		keyFields[keyPosition-1] = true
	}

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
			output(lastTs, lastKeys, lastValues)
			lastKeys, lastValues = keys, values
		}
		if row.T > lastTs {
			lastTs = row.T
		}
	}
	output(lastTs, lastKeys, lastValues)

	return nil
}

func output(ts int64, x, y []interface{}) error {
	stat.Stats[0].OutputCounter++
	var t []interface{}
	t = append(t, x...)
	t = append(t, y...)
	return util.NewRow(ts, t...).WriteTo(os.Stdout)
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
