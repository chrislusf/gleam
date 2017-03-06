package executor

import (
	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/sql/context"
	"github.com/chrislusf/gleam/sql/expression"
)

type ProjectionExec struct {
	Src      Executor
	schema   expression.Schema
	executed bool
	ctx      context.Context
	exprs    []expression.Expression
}

// Schema implements the Executor Schema interface.
func (e *ProjectionExec) Schema() expression.Schema {
	return e.schema
}

// Next implements the Executor Next interface.
func (e *ProjectionExec) Exec(datasets ...*flow.Dataset) *flow.Dataset {
	d := e.Src.Exec()

	ret := d

	return ret
}
