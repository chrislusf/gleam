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
func (e *LimitExec) Exec() *flow.Dataset {
	d := e.Src.Exec()

	k := int(e.Count + e.Offset)

	ret := d.LocalLimit("limit", k, 0)
	if len(d.Shards) > 1 {
		ret = ret.MergeTo("merge", 1)
	}
	ret = ret.LocalLimit("limit", int(e.Count), int(e.Offset))
	return ret
}
