package sql

import (
	"fmt"
	"os"
	"testing"

	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/sql/executor"
	"github.com/chrislusf/gleam/sql/mysql"
)

func TestQuery(t *testing.T) {
	sql := `
    select word
    from words
    limit 2
    `
	f := flow.New()

	ds := f.Strings([]string{
		"this",
		"is",
		"a",
		"table",
	})

	RegisterTable(ds, "words", []executor.TableColumn{
		{"word", mysql.TypeVarchar},
	})

	out, err := Query(sql)
	if err != nil {
		fmt.Printf("error: %s\n", err)
		return
	}

	out.Fprintf(os.Stdout, "%s\n")

	f.Run()

}
