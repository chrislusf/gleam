package agent

import (
	"github.com/chrislusf/gleam/distributed/driver"
	"github.com/chrislusf/gleam/pb"
)

func (as *AgentServer) handleGetStatusRequest(getStatusRequest *pb.GetStatusRequest) *pb.GetStatusResponse {
	requestId := getStatusRequest.GetStartRequestHash()
	stat := as.localExecutorManager.getExecutorStatus(requestId)

	// println("agent find requestId", requestId)

	reply := &pb.GetStatusResponse{
		StartRequestHash: requestId,
		InputStatuses:    driver.ToProto(stat.InputChannelStatuses),
		OutputStatuses:   driver.ToProto(stat.OutputChannelStatuses),
		RequestTime:      stat.RequestTime.Unix(),
		StartTime:        stat.StartTime.Unix(),
		StopTime:         stat.StopTime.Unix(),
	}

	return reply
}
