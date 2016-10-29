package instruction

import (
	"io"

	"github.com/chrislusf/gleam/distributed/cmd"
)

type Order int

const (
	Ascending  = Order(1)
	Descending = Order(-1)
)

type OrderBy struct {
	Index int   // column index, starting from 1
	Order Order // Ascending or Descending
}

type Stats struct {
	Count int
}

type Instruction interface {
	Name() string
	Function() func(readers []io.Reader, writers []io.Writer, stats *Stats)
	SerializeToCommand() *cmd.Instruction
}
