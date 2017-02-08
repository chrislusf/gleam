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
	var x int
	if key, ok := data.(string); ok {
		x = int(Hash([]byte(key)))
	} else if key, ok := data.([]byte); ok {
		x = int(Hash(key))
	} else if key, ok := data.(uint64); ok {
		x = int(key)
	} else if key, ok := data.(uint32); ok {
		x = int(key)
	} else if key, ok := data.(uint8); ok {
		x = int(key)
	} else if key, ok := data.(int); ok {
		x = key
	} else if key, ok := data.(int8); ok {
		x = int(key)
	} else if key, ok := data.(int64); ok {
		x = int(key)
	} else if key, ok := data.(int32); ok {
		x = int(key)
	}
	return x
}

func PartitionByKeys(shardCount int, data []interface{}) int {
	return HashByKeys(data) % shardCount
}

func LessThan(a interface{}, b interface{}) bool {
	return Compare(a, b) < 0
}

func Compare(a interface{}, b interface{}) (ret int) {
	if x, ok := a.([]interface{}); ok {
		y := b.([]interface{})
		for i := 0; i < len(x); i++ {
			ret = Compare(x[i], y[i])
			if ret != 0 {
				return ret
			}
		}
	} else if x, ok := a.(string); ok {
		if y, y_ok := b.(string); y_ok {
			return strings.Compare(x, y)
		}
		if y, y_ok := b.([]byte); y_ok {
			return strings.Compare(x, string(y))
		}
	} else if x, ok := a.([]byte); ok {
		if y, y_ok := b.([]byte); y_ok {
			return bytes.Compare(x, y)
		}
		if y, y_ok := b.(string); y_ok {
			return bytes.Compare(x, []byte(y))
		}
	} else {
		aIsFloat := isFloat(a)
		bIsFloat := isFloat(b)
		if !aIsFloat && !bIsFloat {
			return int(getInt64(a) - getInt64(b))
		}
		var x, y float64
		if aIsFloat {
			x = getFloat64(a)
		} else {
			x = float64(getInt64(a))
		}
		if bIsFloat {
			y = getFloat64(b)
		} else {
			y = float64(getInt64(b))
		}
		if x < y {
			return -1
		} else if x > y {
			return 1
		} else {
			return 0
		}
	}
	return ret
}

func getInt64(a interface{}) int64 {
	if x, ok := a.(uint64); ok {
		return int64(x)
	} else if x, ok := a.(int64); ok {
		return x
	} else if x, ok := a.(uint32); ok {
		return int64(x)
	} else if x, ok := a.(int32); ok {
		return int64(x)
	} else if x, ok := a.(int); ok {
		return int64(x)
	} else if x, ok := a.(uint8); ok {
		return int64(x)
	} else if x, ok := a.(int8); ok {
		return int64(x)
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
