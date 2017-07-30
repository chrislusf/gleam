package util

type Row struct {
	K []interface{}
	V []interface{}
	T int64
}

func NewRow(timestamp int64, objects ...interface{}) Row {
	return Row{
		T: timestamp,
	}.AppendKey(objects[0]).AppendValue(objects[1:]...)
}

func (row Row) AppendKey(objects ...interface{}) Row {
	row.K = append(row.K, objects...)
	return row
}

func (row Row) AppendValue(objects ...interface{}) Row {
	row.V = append(row.V, objects...)
	return row
}

// UseKeys use the indexes[] specified fields as key fields
// and the rest of fields as value fields
func (row Row) UseKeys(indexes []int) (err error) {
	if indexes == nil {
		return nil
	}
	var keys, values []interface{}
	kLen, vLen := len(row.K), len(row.V)
	used := make([]bool, kLen+vLen)
	for _, x := range indexes {
		if x <= kLen {
			keys = append(keys, row.K[x-1])
		} else {
			keys = append(keys, row.V[x-1-kLen])
		}
		used[x-1] = true
	}
	for i, _ := range row.K {
		if !used[i] {
			values = append(values, row.K[i-1])
		}
	}
	for i, _ := range row.V {
		if !used[i+kLen] {
			values = append(values, row.V[i])
		}
	}
	row.K, row.V = keys, values
	return err
}
