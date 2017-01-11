package tests

import (
	"os"
	"testing"

	"github.com/chrislusf/gleam/flow"
	"github.com/stretchr/testify/assert"
)

func TestCountStringOccurances(t *testing.T) {

	flow.New().
		Strings([]string{"a", "a", "b", "a", "c"}).
		Map(`function(x) return x, 1 end`).
		Sort(flow.Field(1)).
		Fprintf(os.Stdout, "%s %d\n").
		Run()

	var s string
	var count int
	flow.New().
		Strings([]string{"a", "a", "b", "a"}).
		Map(`function(x) return x, 1 end`).
		ReduceBy(`function(i1, i2) return i1 + i2 end`).
		SaveFirstRowTo(&s, &count).
		Run()

	assert.Equal(t, "a", s, "We expect the first row to be 'a'")
	assert.Equal(t, 3, count, "The count should be 3!")
}
