package flow

import (
	"fmt"
	"io"
	"os"

	"github.com/chrislusf/gleam/util"
)

type keyValues struct {
	Keys   []interface{}
	Values []interface{}
}

func getKeyValues(row []interface{}, indexes []int, mask []bool) keyValues {
	var keys, values []interface{}
	for _, x := range indexes {
		keys = append(keys, row[x-1])
	}
	for i := 0; i < len(row); i++ {
		if !mask[i] {
			values = append(values, row[i])
		}
	}
	return keyValues{
		Keys:   keys,
		Values: values,
	}
}

func genKeyFieldsMask(n int, indexes []int) []bool {
	mask := make([]bool, n)
	for _, x := range indexes {
		mask[x-1] = true
	}
	return mask
}

// create a channel to aggregate values of the same key
// automatically close original sorted channel
func newChannelOfValuesWithSameKey(sortedChan io.Reader, indexes []int) chan keyValues {
	writer := make(chan keyValues, 1024)
	go func() {

		defer close(writer)

		row, err := util.ReadRow(sortedChan)
		if err != nil {
			if err != io.EOF {
				fmt.Fprintf(os.Stderr, "join read first row error: %v\n", err)
			}
			return
		}
		// fmt.Printf("join read len=%d, row: %s\n", len(row), row[0])

		keyFieldsMask := genKeyFieldsMask(len(row), indexes)

		firstRow := getKeyValues(row, indexes, keyFieldsMask)

		keyValues := keyValues{
			Keys:   firstRow.Keys,
			Values: []interface{}{firstRow.Values},
		}

		for {
			row, err = util.ReadRow(sortedChan)
			if err != nil {
				if err != io.EOF {
					fmt.Fprintf(os.Stderr, "join read row error: %v", err)
				}
				break
			}
			// fmt.Printf("join read len=%d, row: %s\n", len(row), row[0])
			newRow := getKeyValues(row, indexes, keyFieldsMask)
			x := util.Compare(keyValues.Keys, newRow.Keys)
			if x == 0 {
				keyValues.Values = append(keyValues.Values, newRow.Values)
			} else {
				writer <- keyValues
				keyValues.Keys = newRow.Keys
				keyValues.Values = []interface{}{newRow.Values}
			}
		}
		writer <- keyValues
	}()

	return writer
}
