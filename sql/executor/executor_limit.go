package executor

import (
	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/sql/expression"
)

type LimitExec struct {
	Src    Executor
	Offset uint64
	Count  uint64
	schema expression.Schema
}

// Schema implements the Executor Schema interface.
func (e *LimitExec) Schema() expression.Schema {
	return e.schema
}

// Next implements the Executor Next interface.
func (e *LimitExec) Exec(datasets ...*flow.Dataset) *flow.Dataset {
	d := e.Src.Exec()

	k := int(e.Count + e.Offset)

	ret := d.LocalTop(k)
	if len(d.Shards) > 1 {
		ret = ret.MergeSortedTo(1).LocalLimit(k)
	}
	return ret
}
