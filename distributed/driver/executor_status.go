package driver

import (
	"time"

	"github.com/chrislusf/gleam/distributed/cmd"
	"github.com/chrislusf/gleam/distributed/plan"
	"github.com/chrislusf/gleam/distributed/resource"
	"github.com/chrislusf/gleam/util"
	"github.com/golang/protobuf/proto"
)

type RemoteExecutorStatus struct {
	util.ExecutorStatus
	Allocation resource.Allocation
	taskGroup  *plan.TaskGroup
}

func ToProto(channelStatuses []*util.ChannelStatus) (ret []*cmd.ChannelStatus) {
	for _, stat := range channelStatuses {
		ret = append(ret, &cmd.ChannelStatus{
			Length:    proto.Int64(stat.Length),
			StartTime: proto.Int64(stat.StartTime.Unix()),
			StopTime:  proto.Int64(stat.StopTime.Unix()),
			Name:      proto.String(stat.Name),
		})
	}
	return
}

func FromProto(channelStatuses []*cmd.ChannelStatus) (ret []*util.ChannelStatus) {
	for _, stat := range channelStatuses {
		ret = append(ret, &util.ChannelStatus{
			Length:    stat.GetLength(),
			StartTime: time.Unix(stat.GetStartTime(), 0),
			StopTime:  time.Unix(stat.GetStopTime(), 0),
			Name:      stat.GetName(),
		})
	}
	return
}
