package executor

import (
	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/sql/context"
	"github.com/chrislusf/gleam/sql/expression"
)

type UnionScanExec struct {
	ctx       context.Context
	Src       Executor
	desc      bool
	condition expression.Expression

	schema expression.Schema
}

// Schema implements the Executor Schema interface.
func (e *UnionScanExec) Schema() expression.Schema {
	return e.schema
}

// Next implements the Executor Next interface.
func (e *UnionScanExec) Exec() *flow.Dataset {
	d := e.Src.Exec()

	ret := d

	return ret
}
