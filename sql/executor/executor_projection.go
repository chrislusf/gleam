package executor

import (
	"regexp"
	"strings"

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
func (e *ProjectionExec) Exec() *flow.Dataset {
	d := e.Src.Exec()

	// fmt.Printf("=================\n")
	// fmt.Printf("context: %+v\n", e.ctx)

	var inputs []string
	for _, col := range e.Src.Schema().Columns {
		// fmt.Printf("input: %s TblName:%s DBName:%s FromID:%s %d\n", col, col.TblName, col.DBName, col.FromID, col.Position)
		inputs = append(inputs, col.ColName.String())
	}

	/*
		for _, col := range e.Schema().Columns {
			fmt.Printf("output:%s TblName:%s DBName:%s FromID:%s %d\n", col, col.TblName, col.DBName, col.FromID, col.Position)
		}
	*/

	var outputs []string
	for _, expr := range e.exprs {
		// fmt.Printf("expression: %s %+v\n", expr.String(), reflect.TypeOf(expr))
		outputs = append(outputs, expr.String())
	}

	inputParams := removeTableName(strings.Join(inputs, ","))
	outputParams := removeTableName(strings.Join(outputs, ","))

	if inputParams == outputParams {
		return d
	}

	/*
			sqlText := fmt.Sprintf(`
		        function(%s)
		          return %s
		        end
		    `, inputParams, outputParams)
	*/

	// println(sqlText)

	// ret := d.Map("map", sqlText)
	ret := d

	return ret
}

var re = regexp.MustCompile(`([a-z]+\w*\.)(\w+)`)

func removeTableName(sqlText string) string {
	return re.ReplaceAllString(sqlText, "$2")
}
