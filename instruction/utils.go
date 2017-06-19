package instruction

import (
	"fmt"
	"io"
	"os"

	"github.com/chrislusf/gleam/pb"
	"github.com/chrislusf/gleam/util"
)

func toInts(indexes []int32) []int {
	var ret []int
	for _, x := range indexes {
		ret = append(ret, int(x))
	}
	return ret
}

func toOrderBys(orderBys []*pb.OrderBy) (ret []OrderBy) {
	for _, o := range orderBys {
		ret = append(ret, OrderBy{
			Index: int(o.GetIndex()),
			Order: Order(int(o.GetOrder())),
		})
	}
	return ret
}

type keyValues struct {
	Timestamp int64
	Keys      []interface{}
	Values    []interface{}
}

func genKeyBytesAndValues(input []byte, indexes []int) (keyBytes []byte, values []interface{}, err error) {
	_, keys, values, err := util.DecodeRowKeysValues(input, indexes)
	if err != nil {
		return nil, nil, fmt.Errorf("DecodeRowKeysValues %v: %+v", err, input)
	}
	keyBytes, err = util.EncodeKeys(keys...)
	return keyBytes, values, err
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
func newChannelOfValuesWithSameKey(name string, sortedChan io.Reader, indexes []int) chan keyValues {
	writer := make(chan keyValues, 1024)
	go func() {

		defer close(writer)

		ts, row, err := util.ReadRow(sortedChan)
		if err != nil {
			if err != io.EOF {
				fmt.Fprintf(os.Stderr, "%s join read first row error: %v\n", name, err)
			}
			return
		}
		// fmt.Printf("%s join read len=%d, row: %s\n", name, len(row), row[0])

		keyFieldsMask := genKeyFieldsMask(len(row), indexes)

		firstRow := getKeyValues(row, indexes, keyFieldsMask)

		keyValues := keyValues{
			Timestamp: ts,
			Keys:      firstRow.Keys,
			Values:    []interface{}{firstRow.Values},
		}

		for {
			ts, row, err = util.ReadRow(sortedChan)
			if err != nil {
				if err != io.EOF {
					fmt.Fprintf(os.Stderr, "join read row error: %v", err)
				}
				break
			}
			// fmt.Printf("%s join read len=%d, row: %s\n", name, len(row), row[0])
			newRow := getKeyValues(row, indexes, keyFieldsMask)
			x := util.Compare(keyValues.Keys, newRow.Keys)
			if x == 0 {
				keyValues.Values = append(keyValues.Values, newRow.Values)
			} else {
				writer <- keyValues
				keyValues.Keys = newRow.Keys
				keyValues.Values = []interface{}{newRow.Values}
			}
			keyValues.Timestamp = max(keyValues.Timestamp, ts)
		}
		writer <- keyValues
	}()

	return writer
}

func max(x, y int64) int64 {
	if x > y {
		return x
	}
	return y
}
