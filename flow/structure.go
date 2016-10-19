package flow

import (
	"sync"
	"time"

	"github.com/chrislusf/gleam/script"
	"github.com/chrislusf/gleam/util"
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
)

type NetworkType int

const (
	OneShardToOneShard NetworkType = iota
	OneShardToAllShard
	AllShardToOneShard
	OneShardToEveryNShard
	LinkedNShardToOneShard
	MergeTwoShardToOneShard
)

type FlowContext struct {
	PrevScriptType string
	PrevScriptPart string
	Scripts        map[string]func() script.Script
	Steps          []*Step
	Datasets       []*Dataset
	HashCode       uint32
	Runner         FlowRunner
}

type Dataset struct {
	FlowContext     *FlowContext
	Id              int
	Shards          []*DatasetShard
	Step            *Step
	ReadingSteps    []*Step
	IsPartitionedBy []int
	IsLocalSorted   []OrderBy
	RunLocked
}

type DatasetShard struct {
	Id            int
	Dataset       *Dataset
	ReadingTasks  []*Task
	IncomingChan  *util.Piper
	OutgoingChans []*util.Piper
	Counter       int64
	ReadyTime     time.Time
	CloseTime     time.Time
}

type Step struct {
	Id             int
	FlowContext    *FlowContext
	InputDatasets  []*Dataset
	OutputDataset  *Dataset
	Function       func(*Task)
	Tasks          []*Task
	Name           string
	FunctionType   FunctionType
	NetworkType    NetworkType
	IsOnDriverSide bool
	IsPipe         bool
	Script         script.Script
	Command        *script.Command // used in Pipe()
	Params         map[string]interface{}
	RunLocked
}

type Task struct {
	Id           int
	Step         *Step
	InputShards  []*DatasetShard
	InputChans   []*util.Piper // task specific input chans. InputShard may have multiple reading tasks
	OutputShards []*DatasetShard
}

type RunLocked struct {
	sync.Mutex
	StartTime time.Time
}

type Order int

const (
	Ascending  = Order(1)
	Descending = Order(-1)
)

type OrderBy struct {
	Index int
	Order Order
}
