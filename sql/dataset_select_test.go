package sql

import (
	"fmt"
	"testing"

	"github.com/chrislusf/gleam/sql/parser"
)

func TestSelect(t *testing.T) {
	sql := `select * from users where age > 5`
	p := parser.New()
	tree, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		t.Errorf("parse error: %v", err)
	}

	fmt.Printf("sql:%v\n", tree.Text())

}
