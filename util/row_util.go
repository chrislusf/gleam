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
