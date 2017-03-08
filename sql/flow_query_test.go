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
    limit 105 offset 1
    `
	f := flow.New()

	ds := f.Strings([]string{
		"thisx",
		"is00x",
		"a000x",
		"table",
		"thisy",
		"is00y",
		"a000y",
		"tably",
	}).RoundRobin(2)

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
