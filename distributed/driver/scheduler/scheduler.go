// Schedule tasks to run on available resources assigned by master.
package scheduler

import (
	"os"
	"os/user"
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
	Option       *Option
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

type Option struct {
	Username     string
	Hostname     string
	FlowHashcode uint32
	DataCenter   string
	Rack         string
	TaskMemoryMB int
	Module       string
	IsProfiling  bool
}

func New(leader string, option *Option) *Scheduler {
	if currentUser, err := user.Current(); err == nil {
		option.Username = currentUser.Username
	}
	option.Hostname, _ = os.Hostname()

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
