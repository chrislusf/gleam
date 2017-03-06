package executor

import (
	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/sql/expression"
)

type Executor interface {
	Exec() *flow.Dataset
	Schema() expression.Schema
}
