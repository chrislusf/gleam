package sql

import (
	"fmt"
	"os"
	"testing"

	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/sql"
	"github.com/chrislusf/gleam/sql/executor"
	"github.com/chrislusf/gleam/sql/mysql"
	"github.com/chrislusf/gleam/sql/plan"
)

func TestLimitOffset(t *testing.T) {
	sqlText := `
    select word, line
    from words
    limit 2 offset 1
    `
	f := flow.New()

	ds := f.Slices([][]interface{}{
		{"this", 1},
		{"is", 2},
		{"a", 3},
		{"table", 4},
		{"that", 5},
		{"are", 6},
		{"many", 7},
		{"pencils", 6},
	}).RoundRobin(2)

	sql.RegisterTable(ds, "words", []executor.TableColumn{
		{"word", mysql.TypeVarchar},
		{"line", mysql.TypeLong},
	})

	out, p, err := sql.Query(sqlText)
	if err != nil {
		fmt.Printf("error: %s\n", err)
		return
	}

	out.Fprintf(os.Stdout, "%s %d\n")

	f.Run()

	fmt.Printf("plan: %v\n", plan.ToString(p))
}
