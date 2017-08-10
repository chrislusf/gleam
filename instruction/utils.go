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

// create a channel to aggregate values of the same key
// automatically close original sorted channel
func newChannelOfValuesWithSameKey(name string, sortedChan io.Reader, indexes []int) chan util.Row {
	writer := make(chan util.Row, 1024)
	go func() {

		defer close(writer)

		firstRow, err := util.ReadRow(sortedChan)
		if err != nil {
			if err != io.EOF {
				fmt.Fprintf(os.Stderr, "%s join read first row error: %v\n", name, err)
			}
			return
		}
		// fmt.Printf("%s join read len=%d, row: %s\n", name, len(row), row[0])

		firstRow.UseKeys(indexes)

		row := util.Row{
			T: firstRow.T,
			K: firstRow.K,
			V: []interface{}{firstRow.V},
		}

		for {
			newRow, err := util.ReadRow(sortedChan)
			if err != nil {
				if err != io.EOF {
					fmt.Fprintf(os.Stderr, "join read row error: %v", err)
				}
				break
			}
			newRow.UseKeys(indexes)
			x := util.Compare(row.K, newRow.K)
			if x == 0 {
				row.V = append(row.V, newRow.V)
			} else {
				writer <- row
				row.K = newRow.K
				row.V = []interface{}{newRow.V}
			}
			row.T = max(row.T, newRow.T)
		}
		writer <- row
	}()

	return writer
}

func max(x, y int64) int64 {
	if x > y {
		return x
	}
	return y
}
