// Schedule tasks to run on available resources assigned by master.
package scheduler

import (
	"sync"
	"time"

	"github.com/chrislusf/gleam/distributed/driver/scheduler/market"
	"github.com/chrislusf/gleam/pb"
)

type Scheduler struct {
	sync.Mutex

	Master       string
	EventChan    chan interface{}
	Market       *market.Market
	Option       *SchedulerOption
	shardLocator *DatasetShardLocator
}

type RemoteExecutorStatus struct {
	Request      *pb.ExecutionRequest
	Allocation   *pb.Allocation
	RequestTime  time.Time
	InputLength  int
	OutputLength int
	ReadyTime    time.Time
	RunTime      time.Time
	StopTime     time.Time
}

type SchedulerOption struct {
	DataCenter   string
	Rack         string
	TaskMemoryMB int
	Module       string
}

func NewScheduler(leader string, option *SchedulerOption) *Scheduler {
	s := &Scheduler{
		Master:       leader,
		EventChan:    make(chan interface{}),
		Market:       market.NewMarket(),
		shardLocator: NewDatasetShardLocator(),
		Option:       option,
	}
	s.Market.SetScoreFunction(s.Score).SetFetchFunction(s.Fetch)
	return s
}
