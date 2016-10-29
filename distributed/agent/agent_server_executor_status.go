package agent

import (
	"time"

	"github.com/chrislusf/gleam/msg"
	"github.com/chrislusf/gleam/distributed/driver"
	"github.com/golang/protobuf/proto"
)

func (as *AgentServer) handleGetStatusRequest(getStatusRequest *msg.GetStatusRequest) *msg.GetStatusResponse {
	requestId := getStatusRequest.GetStartRequestHash()
	stat := as.localExecutorManager.getExecutorStatus(requestId)

	reply := &msg.GetStatusResponse{
		StartRequestHash: proto.Uint32(requestId),
		InputStatuses:    driver.ToProto(stat.InputChannelStatuses),
		OutputStatuses:   driver.ToProto(stat.OutputChannelStatuses),
		RequestTime:      proto.Int64(stat.RequestTime.Unix()),
		StartTime:        proto.Int64(stat.StartTime.Unix()),
		StopTime:         proto.Int64(stat.StopTime.Unix()),
	}

	return reply
}

func (as *AgentServer) handleLocalStatusReportRequest(localStatusRequest *msg.LocalStatusReportRequest) *msg.LocalStatusReportResponse {
	requestId := localStatusRequest.GetStartRequestHash()
	stat := as.localExecutorManager.getExecutorStatus(requestId)

	stat.InputChannelStatuses = driver.FromProto(localStatusRequest.GetInputStatuses())
	stat.OutputChannelStatuses = driver.FromProto(localStatusRequest.GetOutputStatuses())
	stat.LastAccessTime = time.Now()

	reply := &msg.LocalStatusReportResponse{}

	return reply
}

func (as *AgentServer) handleStopRequest(stopRequest *msg.StopRequest) *msg.StopResponse {
	requestId := stopRequest.GetStartRequestHash()
	stat := as.localExecutorManager.getExecutorStatus(requestId)

	if stat.Process != nil {
		stat.Process.Kill()
		stat.Process = nil
	}

	reply := &msg.StopResponse{
		StartRequestHash: proto.Uint32(requestId),
	}

	return reply
}
