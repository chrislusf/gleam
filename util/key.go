package util

import (
	"bytes"
	"strings"
)

func HashByKeys(data []interface{}) int {
	var x int
	for _, d := range data {
		x = x*31 + hashByKey(d)
	}
	return x
}

func hashByKey(data interface{}) int {

	switch key := data.(type) {
	case string:
		return int(Hash([]byte(key)))

	case []byte:
		return int(Hash(key))

	case uint64:
		return int(key)

	case uint32:
		return int(key)

	case uint8:
		return int(key)

	case int:
		return key

	case int8:
		return int(key)

	case int32:
		return int(key)

	case int64:
		return int(key)
	}

	return 0
}

func PartitionByKeys(shardCount int, data []interface{}) int {
	return HashByKeys(data) % shardCount
}

func LessThan(a interface{}, b interface{}) bool {
	return Compare(a, b) < 0
}

func toFloat64(isFloat bool, v interface{}) float64 {
	if isFloat {
		return getFloat64(v)
	}
	return float64(getInt64(v))
}
func Compare(a interface{}, b interface{}) (ret int) {
	switch x := a.(type) {
	case []interface{}:
		y := b.([]interface{})
		for i := 0; i < len(x); i++ {
			ret = Compare(x[i], y[i])
			if ret != 0 {
				return ret
			}
		}

	case string:
		if y, ok := b.(string); ok {
			return strings.Compare(x, y)
		}
		if y, ok := b.([]byte); ok {
			return strings.Compare(x, string(y))
		}

	case []byte:
		if y, ok := b.([]byte); ok {
			return bytes.Compare(x, y)
		}
		if y, ok := b.(string); ok {
			return strings.Compare(string(x), y)
		}

	default:
		aIsFloat := isFloat(a)
		bIsFloat := isFloat(b)
		if !aIsFloat && !bIsFloat {
			return int(getInt64(a) - getInt64(b))
		}

		t := toFloat64(aIsFloat, a)
		y := toFloat64(bIsFloat, b)

		if t < y {
			return -1

		} else if t > y {
			return 1
		}
		return 0
	}

	return ret
}

func getInt64(v interface{}) int64 {
	switch t := v.(type) {

	case uint64:
		return int64(t)
	case uint32:
		return int64(t)
	case int32:
		return int64(t)
	case int:
		return int64(t)
	case uint8:
		return int64(t)
	case int8:
		return int64(t)

	case int64:
		return t
	}

	return 0

}
func getFloat64(a interface{}) float64 {
	if x, ok := a.(float64); ok {
		return float64(x)
	}
	return 0.0
}

func isFloat(a interface{}) bool {
	if _, ok := a.(float64); ok {
		return true
	}
	if _, ok := a.(float32); ok {
		return true
	}
	return false
}
