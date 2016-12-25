package driver

import (
	"time"

	"github.com/chrislusf/gleam/distributed/plan"
	"github.com/chrislusf/gleam/pb"
	"github.com/chrislusf/gleam/util"
)

type RemoteExecutorStatus struct {
	util.ExecutorStatus
	Allocation *pb.Allocation
	taskGroup  *plan.TaskGroup
}

func ToProto(channelStatuses []*util.ChannelStatus) (ret []*pb.ChannelStatus) {
	for _, stat := range channelStatuses {
		ret = append(ret, &pb.ChannelStatus{
			Length:    stat.Length,
			StartTime: stat.StartTime.Unix(),
			StopTime:  stat.StopTime.Unix(),
			Name:      stat.Name,
		})
	}
	return
}

func FromProto(channelStatuses []*pb.ChannelStatus) (ret []*util.ChannelStatus) {
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
