package util

import (
	"bytes"
	"strings"
)

func HashByKey(data interface{}, shardCount int) int {
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
	return x % shardCount
}

func LessThan(a interface{}, b interface{}) bool {
	return Compare(a, b) < 0
}

func Compare(a interface{}, b interface{}) (ret int) {
	if x, ok := a.(string); ok {
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
