package agent

import (
	"time"

	"github.com/chrislusf/gleam/distributed/driver"
	"github.com/chrislusf/gleam/pb"
)

func (as *AgentServer) handleGetStatusRequest(getStatusRequest *pb.GetStatusRequest) *pb.GetStatusResponse {
	requestId := getStatusRequest.GetStartRequestHash()
	stat := as.localExecutorManager.getExecutorStatus(requestId)

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

func (as *AgentServer) handleLocalStatusReportRequest(localStatusRequest *pb.LocalStatusReportRequest) *pb.LocalStatusReportResponse {
	requestId := localStatusRequest.GetStartRequestHash()
	stat := as.localExecutorManager.getExecutorStatus(requestId)

	stat.InputChannelStatuses = driver.FromProto(localStatusRequest.GetInputStatuses())
	stat.OutputChannelStatuses = driver.FromProto(localStatusRequest.GetOutputStatuses())
	stat.LastAccessTime = time.Now()

	reply := &pb.LocalStatusReportResponse{}

	return reply
}

func (as *AgentServer) handleStopRequest(stopRequest *pb.StopRequest) *pb.StopResponse {
	requestId := stopRequest.GetStartRequestHash()
	stat := as.localExecutorManager.getExecutorStatus(requestId)

	if stat.Process != nil {
		stat.Process.Kill()
		stat.Process = nil
	}

	reply := &pb.StopResponse{
		StartRequestHash: requestId,
	}

	return reply
}
