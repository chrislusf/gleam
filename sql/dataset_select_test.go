package sql

import (
	"fmt"
	"testing"

	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/sql/executor"
	"github.com/chrislusf/gleam/sql/infoschema"
	"github.com/chrislusf/gleam/sql/mysql"
	"github.com/chrislusf/gleam/sql/parser"
	"github.com/chrislusf/gleam/sql/plan"
)

func xTestSelect(t *testing.T) {
	sql := `
    select a.age, count(*) c
    from (
        select age 
        from users 
        where age > 5
    ) a 
    group by age 
    order by count(*) desc
    `
	p := parser.New()
	tree, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		t.Errorf("parse error: %v", err)
	}

	fmt.Printf("sql:%v\n", tree.Text())

	var ds *flow.Dataset

	RegisterTable(ds, "users", []executor.TableColumn{
		{"age", mysql.TypeVarchar},
	})

	infoSchema := infoschema.NewInfoSchema("", tableInfoList())

	session, err := CreateSession(infoSchema)
	if err != nil {
		t.Errorf("session error: %v", err)
	}
	physicalPlan, err := Compile(session, tree)
	if err != nil {
		t.Errorf("compile error: %v", err)
	}

	fmt.Printf("plan:%v\n", plan.ToString(physicalPlan))

}
