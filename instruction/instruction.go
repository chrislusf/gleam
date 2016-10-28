package instruction

import (
	"io"

	"github.com/chrislusf/gleam/distributed/cmd"
)

type FunctionType int

const (
	TypeScript FunctionType = iota
	TypeLocalSort
	TypeMergeSortedTo
	TypeJoinPartitionedSorted
	TypeCoGroupPartitionedSorted
	TypeCollectPartitions
	TypeScatterPartitions
	TypeRoundRobin
	TypePipeAsArgs
	TypeInputSplitReader
	TypeLocalTop
	TypeBroadcast
	TypeLocalHashAndJoinWith
)

type Stats struct {
	Count int
}

type Instruction interface {
	Name() string
	FunctionType() FunctionType
	Function() func(readers []io.Reader, writers []io.Writer, stats *Stats)
	SerializeToCommand() *cmd.Instruction
}
