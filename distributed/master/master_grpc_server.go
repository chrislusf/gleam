package master

import (
	"fmt"
	"io"

	"github.com/chrislusf/gleam/pb"
	"golang.org/x/net/context"
)

type MasterServer struct {
	Topology *Topology
}

func newMasterServer() *MasterServer {
	return &MasterServer{
		Topology: NewTopology(),
	}
}

func (s *MasterServer) GetResources(ctx context.Context, in *pb.ComputeRequest) (*pb.AllocationResult, error) {
	var err error
	dcName := in.GetDataCenter()
	if dcName == "" {
		dcName, err = s.Topology.allocateDataCenter(in.GetComputeResources())
		if err != nil {
			return nil, err
		}
	}
	dc, hasDc := s.Topology.GetDataCenter(dcName)
	if !hasDc {
		return nil, fmt.Errorf("Failed to find existing data center: %s", dcName)
	}

	Allocations := s.Topology.findServers(dc, in.GetComputeResources())

	return &pb.AllocationResult{
		Allocations: Allocations,
	}, nil

}

func (s *MasterServer) SendHeartbeat(stream pb.GleamMaster_SendHeartbeatServer) error {
	var location *pb.Location
	for {
		heartbeat, err := stream.Recv()
		if err == nil {
			if location == nil {
				location = heartbeat.Location
				fmt.Printf("added agent: %v\n", location)
			}
		} else {
			if location != nil {
				s.Topology.deleteAgentInformation(location)
			}
			fmt.Printf("lost agent: %v\n", location)

			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
		}
		s.Topology.UpdateAgentInformation(heartbeat)
	}
}
