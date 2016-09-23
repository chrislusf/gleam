// Schedule tasks to run on available resources assigned by master.
package scheduler

import (
	"sync"
	"time"

	"github.com/chrislusf/gleam/distributed/cmd"
	"github.com/chrislusf/gleam/distributed/driver/scheduler/market"
	"github.com/chrislusf/gleam/distributed/resource"
)

type Scheduler struct {
	sync.Mutex

	Leader                 string
	EventChan              chan interface{}
	Market                 *market.Market
	Option                 *SchedulerOption
	shardLocator           *DatasetShardLocator
	RemoteExecutorStatuses map[uint32]*RemoteExecutorStatus
}

type RemoteExecutorStatus struct {
	Request      *cmd.ControlMessage
	Allocation   resource.Allocation
	RequestTime  time.Time
	InputLength  int
	OutputLength int
	ReadyTime    time.Time
	RunTime      time.Time
	StopTime     time.Time
}

type SchedulerOption struct {
	DataCenter         string
	Rack               string
	TaskMemoryMB       int
	DriverHost         string
	DriverPort         int
	Module             string
	ExecutableFile     string
	ExecutableFileHash uint32
}

func NewScheduler(leader string, option *SchedulerOption) *Scheduler {
	s := &Scheduler{
		Leader:                 leader,
		EventChan:              make(chan interface{}),
		Market:                 market.NewMarket(),
		shardLocator:           NewDatasetShardLocator(option.ExecutableFileHash),
		Option:                 option,
		RemoteExecutorStatuses: make(map[uint32]*RemoteExecutorStatus),
	}
	s.Market.SetScoreFunction(s.Score).SetFetchFunction(s.Fetch)
	return s
}

func (s *Scheduler) getRemoteExecutorStatus(id uint32) (status *RemoteExecutorStatus, isOld bool) {
	s.Lock()
	defer s.Unlock()

	status, isOld = s.RemoteExecutorStatuses[id]
	if isOld {
		return status, isOld
	}
	status = &RemoteExecutorStatus{}
	s.RemoteExecutorStatuses[id] = status
	return status, false
}
