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
		ret = strings.Compare(x, b.(string))
	} else if x, ok := a.([]byte); ok {
		ret = bytes.Compare(x, b.([]byte))
	} else if x, ok := a.(uint64); ok {
		ret = int(x - b.(uint64))
	} else if x, ok := a.(int64); ok {
		ret = int(x - b.(int64))
	} else if x, ok := a.(float64); ok {
		ret = int(x - b.(float64))
	} else if x, ok := a.(uint32); ok {
		ret = int(x - b.(uint32))
	} else if x, ok := a.(int32); ok {
		ret = int(x - b.(int32))
	} else if x, ok := a.(int); ok {
		ret = x - b.(int)
	} else if x, ok := a.(uint8); ok {
		ret = int(x - b.(uint8))
	} else if x, ok := a.(int8); ok {
		ret = int(x - b.(int8))
	}
	return ret
}
