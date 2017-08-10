package sql

import (
	"fmt"
	"os"
	"testing"

	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/gio"
	"github.com/chrislusf/gleam/sql"
	"github.com/chrislusf/gleam/sql/executor"
	"github.com/chrislusf/gleam/sql/mysql"
	"github.com/chrislusf/gleam/sql/plan"
)

func TestLimitOffset(t *testing.T) {
	gio.Init()

	sqlText := `
    select
      line div l2,
      (line+l2)/2*1.5,
      -- concat(cast(line as char), w ),
      x,
      line in (4,5)
    from
    (
    select line, word as w, line as l2, word is null as x
    from words
    limit 2 offset 1
    ) a
    `
	f := flow.New("testLimit")

	ds := f.Slices([][]interface{}{
		{"this", 1},
		{"is", 2},
		{"a", 3},
		{"table", 4},
		{"that", 5},
		{"are", 6},
		{"many", 7},
		{"pencils", 6},
	}).RoundRobin("rr", 2)

	sql.RegisterTable(ds, "words", []executor.TableColumn{
		{"word", mysql.TypeVarchar},
		{"line", mysql.TypeLong},
	})

	out, p, err := sql.Query(sqlText)
	if err != nil {
		fmt.Printf("error: %s\n", err)
		return
	}

	out.Fprintf(os.Stdout, "%d %d: %v, %v\n")

	f.Run()

	fmt.Printf("plan: %v\n", plan.ToString(p))
}
