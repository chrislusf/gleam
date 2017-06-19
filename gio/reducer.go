package gio

import (
	"fmt"
	"io"
	"os"

	"github.com/chrislusf/gleam/util"
)

func ProcessReducer(f Reducer, keyPositions []int) (err error) {

	keyFields := make([]bool, len(keyPositions))
	for _, keyPosition := range keyPositions {
		// change from 1-base to 0-base
		keyFields[keyPosition-1] = true
	}

	// get the first row
	ts, row, err := util.ReadRow(os.Stdin)
	if err != nil {
		if err == io.EOF {
			return nil
		}
		return fmt.Errorf("input row error: %v", err)
	}

	lastTs := ts
	lastKeys, lastValues := getKeysAndValues(row, keyFields)

	for {
		ts, row, err = util.ReadRow(os.Stdin)
		if err != nil {
			if err != io.EOF {
				fmt.Fprintf(os.Stderr, "join read row error: %v", err)
			}
			break
		}

		keys, values := getKeysAndValues(row, keyFields)
		x := util.Compare(lastKeys, keys)
		if x == 0 {
			lastValues, err = reduce(f, lastValues, values)
		} else {
			output(lastTs, lastKeys, lastValues)
			lastKeys, lastValues = keys, values
		}
		if ts > lastTs {
			lastTs = ts
		}
	}
	output(lastTs, lastKeys, lastValues)

	return nil
}

func output(ts int64, x, y []interface{}) error {
	var t []interface{}
	t = append(t, x...)
	t = append(t, y...)
	return util.WriteRow(os.Stdout, ts, t...)
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

func getKeysAndValues(row []interface{}, keyFields []bool) (keys, values []interface{}) {
	for i, data := range row {
		if i < len(keyFields) && keyFields[i] {
			keys = append(keys, data)
		} else {
			values = append(values, data)
		}
	}
	return keys, values
}
