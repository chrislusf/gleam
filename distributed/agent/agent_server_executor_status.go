package agent

import (
	"time"

	"github.com/chrislusf/gleam/distributed"
	"github.com/chrislusf/gleam/distributed/cmd"
	"github.com/golang/protobuf/proto"
)

func (as *AgentServer) handleGetStatusRequest(getStatusRequest *cmd.GetStatusRequest) *cmd.GetStatusResponse {
	requestId := getStatusRequest.GetStartRequestHash()
	stat := as.localExecutorManager.getExecutorStatus(requestId)

	reply := &cmd.GetStatusResponse{
		StartRequestHash: proto.Uint32(requestId),
		InputStatuses:    distributed.ToProto(stat.InputChannelStatuses),
		OutputStatuses:   distributed.ToProto(stat.OutputChannelStatuses),
		RequestTime:      proto.Int64(stat.RequestTime.Unix()),
		StartTime:        proto.Int64(stat.StartTime.Unix()),
		StopTime:         proto.Int64(stat.StopTime.Unix()),
	}

	return reply
}

func (as *AgentServer) handleLocalStatusReportRequest(localStatusRequest *cmd.LocalStatusReportRequest) *cmd.LocalStatusReportResponse {
	requestId := localStatusRequest.GetStartRequestHash()
	stat := as.localExecutorManager.getExecutorStatus(requestId)

	stat.InputChannelStatuses = distributed.FromProto(localStatusRequest.GetInputStatuses())
	stat.OutputChannelStatuses = distributed.FromProto(localStatusRequest.GetOutputStatuses())
	stat.LastAccessTime = time.Now()

	reply := &cmd.LocalStatusReportResponse{}

	return reply
}

func (as *AgentServer) handleStopRequest(stopRequest *cmd.StopRequest) *cmd.StopResponse {
	requestId := stopRequest.GetStartRequestHash()
	stat := as.localExecutorManager.getExecutorStatus(requestId)

	if stat.Process != nil {
		stat.Process.Kill()
		stat.Process = nil
	}

	reply := &cmd.StopResponse{
		StartRequestHash: proto.Uint32(requestId),
	}

	return reply
}
