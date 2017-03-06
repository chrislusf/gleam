package executor

import (
	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/sql/context"
	"github.com/chrislusf/gleam/sql/expression"
	"github.com/chrislusf/gleam/sql/model"
	"github.com/chrislusf/gleam/sql/plan"
	"github.com/chrislusf/gleam/sql/table"
	"github.com/chrislusf/gleam/sql/util/types"
)

type SelectTableExec struct {
	tableInfo *model.TableInfo
	table     table.Table
	asName    *model.CIStr
	ctx       context.Context
	result    *flow.Dataset

	// where        *tipb.Expr
	Columns      []*model.ColumnInfo
	schema       expression.Schema
	ranges       []plan.TableRange
	desc         bool
	limitCount   *int64
	returnedRows uint64 // returned rowCount
	keepOrder    bool
	startTS      uint64
	// orderByList  []*tipb.ByItem

	/*
	   The following attributes are used for aggregation push down.
	   aggFuncs is the aggregation functions in protobuf format. They will be added to distsql request msg.
	   byItem is the groupby items in protobuf format. They will be added to distsql request msg.
	   aggFields is used to decode returned rows from distsql.
	   aggregate indicates of the executor is handling aggregate result.
	   It is more convenient to use a single varible than use a long condition.
	*/
	// aggFuncs  []*tipb.Expr
	// byItems   []*tipb.ByItem
	aggFields []*types.FieldType
	aggregate bool
}

// Schema implements the Executor Schema interface.
func (e *SelectTableExec) Schema() expression.Schema {
	return e.schema
}

// Next implements the Executor Next interface.
func (e *SelectTableExec) Exec() *flow.Dataset {

	t := Tables[e.tableInfo.Name.String()]

	d := t.Dataset

	return d
}
