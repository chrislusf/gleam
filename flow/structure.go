package flow

import (
	"sync"
	"time"

	"github.com/chrislusf/gleam/script"
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
}

type Dataset struct {
	FlowContext      *FlowContext
	Id               int
	Shards           []*DatasetShard
	Step             *Step
	ReadingSteps     []*Step
	IsKeyPartitioned bool
	IsLocalSorted    bool
	RunLocked
}

type DatasetShard struct {
	Id            int
	Dataset       *Dataset
	ReadingTasks  []*Task
	IncomingChan  chan []byte
	OutgoingChans []chan []byte
	Counter       int
	ReadyTime     time.Time
	CloseTime     time.Time
}

type Step struct {
	Id            int
	FlowContext   *FlowContext
	InputDatasets []*Dataset
	OutputDataset *Dataset
	Function      func(*Task)
	Tasks         []*Task
	Name          string
	NetworkType   NetworkType
	IsPipe        bool
	Script        script.Script
	Command       *script.Command
	RunLocked
}

type Task struct {
	Id           int
	Step         *Step
	InputShards  []*DatasetShard
	OutputShards []*DatasetShard
}

type RunLocked struct {
	sync.Mutex
	StartTime time.Time
}
