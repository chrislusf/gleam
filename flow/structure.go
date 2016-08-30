package flow

import (
	"sync"
	"time"

	"github.com/chrislusf/gleam/script"
)

type NetworkType int

const (
	OneShardToOneShard NetworkType = iota
)

type FlowContext struct {
	LastScriptType string
	Scripts        map[string]script.Script
	Steps          []*Step
	Datasets       []*Dataset
}

type Dataset struct {
	FlowContext  *FlowContext
	Shards       []*DatasetShard
	Step         *Step
	ReadingSteps []*Step
	RunLocked
}

type DatasetShard struct {
	Dataset       *Dataset
	ReadingTasks  []*Task
	IncomingChan  chan []byte
	OutgoingChans []chan []byte
	ReadyTime     time.Time
	CloseTime     time.Time
}

type Step struct {
	FlowContext *FlowContext
	Inputs      []*Dataset
	Output      *Dataset
	Function    func(*Task)
	Tasks       []*Task
	Name        string
	NetworkType NetworkType
	Script      script.Script
	RunLocked
}

type Task struct {
	Step    *Step
	Inputs  []*DatasetShard
	Outputs []*DatasetShard
}

type RunLocked struct {
	sync.Mutex
	StartTime time.Time
}
