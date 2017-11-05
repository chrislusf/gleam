package util

import (
	"bytes"
	"math"
	"strings"
)

func HashByKeys(data []interface{}) int {
	var x int
	for _, d := range data {
		x = x*31 + hashByKey(d)
	}
	return x & math.MaxInt64
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
			return int(ToInt64(a) - ToInt64(b))
		}

		t := ToFloat64(a)
		y := ToFloat64(b)

		if t < y {
			return -1

		} else if t > y {
			return 1
		}
		return 0
	}

	return ret
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
