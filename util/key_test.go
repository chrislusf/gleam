package util

import (
	"fmt"
	"testing"
)

func TestHashPositive(t *testing.T) {
	cases := [][]interface{}{
		{int(-1)},
		{uint32(0xffffffff)},
		{uint64(0xffffffffffffffff)},
		{"1ne", "for", "the", "silver", "two", "for", "the", "gold"},
	}
	for _, c := range cases {
		h := HashByKeys(c)
		fmt.Printf("%x\n", h)
		if h < 0 {
			t.Error(c, " has negative hash ", h)
		}
	}
}
