// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"github.com/chrislusf/gleam/sql/ast"
	"github.com/chrislusf/gleam/sql/context"
	"github.com/chrislusf/gleam/sql/infoschema"
	"github.com/chrislusf/gleam/sql/mysql"
	"github.com/chrislusf/gleam/sql/terror"
	"github.com/juju/errors"
)

// AllowCartesianProduct means whether tidb allows cartesian join without equal conditions.
var AllowCartesianProduct = true

// Optimize does optimization and creates a Plan.
// The node must be prepared first.
func Optimize(ctx context.Context, node ast.Node, is infoschema.InfoSchema) (Plan, error) {
	// We have to infer type again because after parameter is set, the expression type may change.
	if err := InferType(ctx.GetSessionVars().StmtCtx, node); err != nil {
		return nil, errors.Trace(err)
	}
	allocator := new(idAllocator)
	builder := &planBuilder{
		ctx:       ctx,
		is:        is,
		colMapper: make(map[*ast.ColumnNameExpr]int),
		allocator: allocator}
	p := builder.build(node)
	if builder.err != nil {
		return nil, errors.Trace(builder.err)
	}
	if logic, ok := p.(LogicalPlan); ok {
		return doOptimize(logic, ctx, allocator)
	}
	return p, nil
}

func doOptimize(logic LogicalPlan, ctx context.Context, allocator *idAllocator) (PhysicalPlan, error) {
	var err error
	logic = decorrelate(logic)
	_, logic, err = logic.PredicatePushDown(nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	solver := &aggPushDownSolver{
		ctx:   ctx,
		alloc: allocator,
	}
	solver.aggPushDown(logic)
	logic.PruneColumns(logic.GetSchema().Columns)
	logic.ResolveIndicesAndCorCols()
	if !AllowCartesianProduct && existsCartesianProduct(logic) {
		return nil, errors.Trace(ErrCartesianProductUnsupported)
	}
	logic.buildKeyInfo()
	info, err := logic.convert2PhysicalPlan(&requiredProperty{})
	if err != nil {
		return nil, errors.Trace(err)
	}
	pp := info.p
	return pp, nil
}

func existsCartesianProduct(p LogicalPlan) bool {
	if join, ok := p.(*Join); ok && len(join.EqualConditions) == 0 {
		return join.JoinType == InnerJoin || join.JoinType == LeftOuterJoin || join.JoinType == RightOuterJoin
	}
	for _, child := range p.GetChildren() {
		if existsCartesianProduct(child.(LogicalPlan)) {
			return true
		}
	}
	return false
}

// Optimizer error codes.
const (
	CodeOperandColumns      terror.ErrCode = 1
	CodeInvalidWildCard     terror.ErrCode = 3
	CodeUnsupported         terror.ErrCode = 4
	CodeInvalidGroupFuncUse terror.ErrCode = 5
	CodeIllegalReference    terror.ErrCode = 6
)

// Optimizer base errors.
var (
	ErrOperandColumns              = terror.ClassOptimizer.New(CodeOperandColumns, "Operand should contain %d column(s)")
	ErrInvalidWildCard             = terror.ClassOptimizer.New(CodeInvalidWildCard, "Wildcard fields without any table name appears in wrong place")
	ErrCartesianProductUnsupported = terror.ClassOptimizer.New(CodeUnsupported, "Cartesian product is unsupported")
	ErrInvalidGroupFuncUse         = terror.ClassOptimizer.New(CodeInvalidGroupFuncUse, "Invalid use of group function")
	ErrIllegalReference            = terror.ClassOptimizer.New(CodeIllegalReference, "Illegal reference")
)

func init() {
	mySQLErrCodes := map[terror.ErrCode]uint16{
		CodeOperandColumns:      mysql.ErrOperandColumns,
		CodeInvalidWildCard:     mysql.ErrParse,
		CodeInvalidGroupFuncUse: mysql.ErrInvalidGroupFuncUse,
		CodeIllegalReference:    mysql.ErrIllegalReference,
	}
	terror.ErrClassToMySQLCodes[terror.ClassOptimizer] = mySQLErrCodes
}
