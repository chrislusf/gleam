package executor

import (
	"fmt"

	"github.com/chrislusf/gleam/sql/context"
	"github.com/chrislusf/gleam/sql/infoschema"
	"github.com/chrislusf/gleam/sql/model"
	"github.com/chrislusf/gleam/sql/plan"
)

// executorBuilder builds an Executor from a Plan.
// The InfoSchema must not change during execution.
type executorBuilder struct {
	ctx context.Context
	is  infoschema.InfoSchema
	// If there is any error during Executor building process, err is set.
	err error
}

func newExecutorBuilder(ctx context.Context, is infoschema.InfoSchema) *executorBuilder {
	return &executorBuilder{
		ctx: ctx,
		is:  is,
	}
}

func (b *executorBuilder) build(p plan.Plan) Executor {
	switch v := p.(type) {
	case nil:
		b.err = fmt.Errorf("Unknown Plan %T", p)
		return nil
	case *plan.CheckTable:
		b.err = fmt.Errorf("Unknown Plan %T", p)
		return nil
	case *plan.DDL:
		b.err = fmt.Errorf("Unknown Plan %T", p)
		return nil
	case *plan.Deallocate:
		b.err = fmt.Errorf("Unknown Plan %T", p)
		return nil
	case *plan.Delete:
		b.err = fmt.Errorf("Unknown Plan %T", p)
		return nil
	case *plan.Execute:
		b.err = fmt.Errorf("Unknown Plan %T", p)
		return nil
	case *plan.Explain:
		b.err = fmt.Errorf("Unknown Plan %T", p)
		return nil
	case *plan.Insert:
		b.err = fmt.Errorf("Unknown Plan %T", p)
		return nil
	case *plan.LoadData:
		b.err = fmt.Errorf("Unknown Plan %T", p)
		return nil
	case *plan.Limit:
		return b.buildLimit(v)
	case *plan.Prepare:
		b.err = fmt.Errorf("Unknown Plan %T", p)
		return nil
	case *plan.SelectLock:
		b.err = fmt.Errorf("Unknown Plan %T", p)
		return nil
	case *plan.ShowDDL:
		b.err = fmt.Errorf("Unknown Plan %T", p)
		return nil
	case *plan.Show:
		b.err = fmt.Errorf("Unknown Plan %T", p)
		return nil
	case *plan.Simple:
		b.err = fmt.Errorf("Unknown Plan %T", p)
		return nil
	case *plan.Set:
		b.err = fmt.Errorf("Unknown Plan %T", p)
		return nil
	case *plan.Sort:
		b.err = fmt.Errorf("Unknown Plan %T", p)
		return b.buildSort(v)
	case *plan.Union:
		b.err = fmt.Errorf("Unknown Plan %T", p)
		return b.buildUnion(v)
	case *plan.Update:
		b.err = fmt.Errorf("Unknown Plan %T", p)
		return nil
	case *plan.PhysicalUnionScan:
		return b.buildUnionScanExec(v)
	case *plan.PhysicalHashJoin:
		b.err = fmt.Errorf("Unknown Plan %T", p)
		return b.buildJoin(v)
	case *plan.PhysicalHashSemiJoin:
		b.err = fmt.Errorf("Unknown Plan %T", p)
		return nil // b.buildSemiJoin(v)
	case *plan.Selection:
		b.err = fmt.Errorf("Unknown Plan %T", p)
		return b.buildSelection(v)
	case *plan.PhysicalAggregation:
		b.err = fmt.Errorf("Unknown Plan %T", p)
		return b.buildAggregation(v)
	case *plan.Projection:
		return b.buildProjection(v)
	case *plan.PhysicalMemTable:
		b.err = fmt.Errorf("Unknown Plan %T", p)
		return nil
	case *plan.PhysicalTableScan:
		return b.buildTableScan(v)
	case *plan.PhysicalIndexScan:
		b.err = fmt.Errorf("Unknown Plan %T", p)
		return nil
	case *plan.TableDual:
		b.err = fmt.Errorf("Unknown Plan %T", p)
		return b.buildTableDual(v)
	case *plan.PhysicalApply:
		b.err = fmt.Errorf("Unknown Plan %T", p)
		return b.buildApply(v)
	case *plan.Exists:
		b.err = fmt.Errorf("Unknown Plan %T", p)
		return nil
	case *plan.MaxOneRow:
		b.err = fmt.Errorf("Unknown Plan %T", p)
		return nil
	case *plan.Trim:
		b.err = fmt.Errorf("Unknown Plan %T", p)
		return nil
	case *plan.PhysicalDummyScan:
		b.err = fmt.Errorf("Unknown Plan %T", p)
		return nil
	case *plan.Cache:
		b.err = fmt.Errorf("Unknown Plan %T", p)
		return nil
	default:
		b.err = fmt.Errorf("Unknown Plan %T", p)
		return nil
	}
}

func (b *executorBuilder) buildLimit(v *plan.Limit) Executor {
	src := b.build(v.GetChildByIndex(0))
	e := &LimitExec{
		Src:    src,
		Offset: v.Offset,
		Count:  v.Count,
		schema: v.GetSchema(),
	}
	return e
}

func (b *executorBuilder) buildUnionScanExec(v *plan.PhysicalUnionScan) Executor {
	src := b.build(v.GetChildByIndex(0))
	if b.err != nil {
		return nil
	}
	us := &UnionScanExec{ctx: b.ctx, Src: src, schema: v.GetSchema()}
	switch x := src.(type) {
	case *SelectTableExec:
		us.desc = x.desc
		us.condition = v.Condition
		/*
			case *XSelectIndexExec:
				us.desc = x.indexPlan.Desc
				for _, ic := range x.indexPlan.Index.Columns {
					for i, col := range x.indexPlan.GetSchema().Columns {
						if col.ColName.L == ic.Name.L {
							us.usedIndex = append(us.usedIndex, i)
							break
						}
					}
				}
				us.dirty = getDirtyDB(b.ctx).getDirtyTable(x.table.Meta().ID)
				us.condition = v.Condition
				us.buildAndSortAddedRows(x.table, x.asName)
		*/
	default:
		// The mem table will not be written by sql directly, so we can omit the union scan to avoid err reporting.
		return src
	}
	return us
}
func (b *executorBuilder) buildJoin(v *plan.PhysicalHashJoin) Executor {
	return nil
}

func (b *executorBuilder) buildAggregation(v *plan.PhysicalAggregation) Executor {
	return nil
}

func (b *executorBuilder) buildSelection(v *plan.Selection) Executor {
	return nil
}

func (b *executorBuilder) buildProjection(v *plan.Projection) Executor {
	return &ProjectionExec{
		Src:    b.build(v.GetChildByIndex(0)),
		ctx:    b.ctx,
		exprs:  v.Exprs,
		schema: v.GetSchema(),
	}
}

func (b *executorBuilder) buildTableDual(v *plan.TableDual) Executor {
	return nil
}

func (b *executorBuilder) buildTableScan(v *plan.PhysicalTableScan) Executor {
	table, _ := b.is.TableByName(model.NewCIStr(""), v.Table.Name)
	st := &SelectTableExec{
		tableInfo:  v.Table,
		ctx:        b.ctx,
		asName:     v.TableAsName,
		table:      table,
		schema:     v.GetSchema(),
		Columns:    v.Columns,
		ranges:     v.Ranges,
		desc:       v.Desc,
		limitCount: v.LimitCount,
		keepOrder:  v.KeepOrder,
		// where:       v.TableConditionPBExpr,
		aggregate: v.Aggregated,
		// aggFuncs:    v.AggFuncsPB,
		aggFields: v.AggFields,
		// byItems:     v.GbyItemsPB,
		// orderByList: v.SortItemsPB,
	}
	return st
}

func (b *executorBuilder) buildSort(v *plan.Sort) Executor {
	return nil
}

func (b *executorBuilder) buildApply(v *plan.PhysicalApply) Executor {
	return nil
}

func (b *executorBuilder) buildUnion(v *plan.Union) Executor {
	return nil
}
