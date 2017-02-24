package sql

import (
	"fmt"
	"testing"

	"github.com/chrislusf/gleam/sql/infoschema"
	"github.com/chrislusf/gleam/sql/model"
	"github.com/chrislusf/gleam/sql/mysql"
	"github.com/chrislusf/gleam/sql/parser"
	"github.com/chrislusf/gleam/sql/util/types"
)

func TestSelect(t *testing.T) {
	sql := `select * from users where age > 5`
	p := parser.New()
	tree, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		t.Errorf("parse error: %v", err)
	}

	users_table := &model.TableInfo{
		Name: model.NewCIStr("users"),
		Columns: []*model.ColumnInfo{
			&model.ColumnInfo{
				Name:      model.NewCIStr("age"),
				FieldType: *types.NewFieldType(mysql.TypeVarchar),
			},
		},
		Indices: nil,
	}

	infoSchema := infoschema.NewInfoSchema("", []*model.TableInfo{users_table})

	session, err := CreateSession(infoSchema)
	if err != nil {
		t.Errorf("session error: %v", err)
	}
	tree, err = Compile(session, tree)
	if err != nil {
		t.Errorf("compile error: %v", err)
	}

	fmt.Printf("sql:%v\n", tree.Text())

}
