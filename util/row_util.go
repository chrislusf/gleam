package util

import (
	"time"
)

func Now() (ts int64) {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

func max(indexes []int) int {
	m := indexes[0]
	for _, x := range indexes {
		if x > m {
			m = x
		}
	}
	return m
}

func toInt64(val interface{}) int64 {
	if v, ok := val.(int64); ok {
		return int64(v)
	}
	if v, ok := val.(uint64); ok {
		return int64(v)
	}
	if v, ok := val.(int32); ok {
		return int64(v)
	}
	if v, ok := val.(uint32); ok {
		return int64(v)
	}
	if v, ok := val.(int); ok {
		return int64(v)
	}
	if v, ok := val.(uint); ok {
		return int64(v)
	}
	if v, ok := val.(int16); ok {
		return int64(v)
	}
	if v, ok := val.(uint16); ok {
		return int64(v)
	}
	if v, ok := val.(int8); ok {
		return int64(v)
	}
	if v, ok := val.(uint8); ok {
		return int64(v)
	}
	return 0
}
