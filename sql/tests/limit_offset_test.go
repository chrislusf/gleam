package sql

import (
	"fmt"
	"os"
	"testing"

	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/sql"
	"github.com/chrislusf/gleam/sql/executor"
	"github.com/chrislusf/gleam/sql/mysql"
)

func TestLimitOffset(t *testing.T) {
	sqlText := `
    select word
    from words
    limit 2 offset 1
    `
	f := flow.New()

	ds := f.Strings([]string{
		"this",
		"is",
		"a",
		"table",
		"that",
		"are",
		"many",
		"pencils",
	}).RoundRobin(2)

	sql.RegisterTable(ds, "words", []executor.TableColumn{
		{"word", mysql.TypeVarchar},
	})

	out, err := sql.Query(sqlText)
	if err != nil {
		fmt.Printf("error: %s\n", err)
		return
	}

	out.Fprintf(os.Stdout, "%s\n")

	f.Run()

}
