package sql

import (
	"fmt"

	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/sql/executor"
	"github.com/chrislusf/gleam/sql/infoschema"
	"github.com/chrislusf/gleam/sql/model"
	"github.com/chrislusf/gleam/sql/parser"
	"github.com/chrislusf/gleam/sql/plan"
	"github.com/chrislusf/gleam/sql/util/types"
)

func RegisterTable(dataset *flow.Dataset, tableName string, columns []executor.TableColumn) {
	var cols []*model.ColumnInfo
	for _, c := range columns {
		cols = append(cols, &model.ColumnInfo{
			Name:      model.NewCIStr(c.ColumnName),
			FieldType: *types.NewFieldType(c.ColumnType),
		})
	}
	t := &model.TableInfo{
		Name:    model.NewCIStr(tableName),
		Columns: cols,
	}
	executor.Tables[tableName] = &executor.TableSource{
		Dataset:   dataset,
		TableInfo: t,
	}
}

func tableInfoList() (infos []*model.TableInfo) {
	for _, ts := range executor.Tables {
		t := ts.TableInfo
		infos = append(infos, t)
	}
	return infos
}

func Query(sql string) (*flow.Dataset, plan.Plan, error) {
	p := parser.New()
	tree, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		return nil, nil, fmt.Errorf("Failed to parse SQL %s: %v", sql, err)
	}

	infoSchema := infoschema.NewInfoSchema("", tableInfoList())

	session, err := CreateSession(infoSchema)
	if err != nil {
		return nil, nil, fmt.Errorf("Failed to create session %v", err)
	}

	physicalPlan, err := Compile(session, tree)
	if err != nil {
		return nil, nil, fmt.Errorf("Failed to get physical plan for %s: %v", sql, err)
	}

	sa := &executor.Statement{
		InfoSchema: infoSchema,
		Plan:       physicalPlan,
		Text:       tree.Text(),
	}

	ds, err := sa.Exec(session)

	return ds, physicalPlan, err

}
