package master

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/chrislusf/gleam/pb"
	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/golang-lru"
	"golang.org/x/net/context"
)

type MasterServer struct {
	Topology     *Topology
	statusCache  *lru.Cache
	logDirectory string
	startTime    time.Time
}

func newMasterServer(logDirectory string) *MasterServer {
	m := &MasterServer{
		Topology:     NewTopology(),
		logDirectory: logDirectory,
		startTime:    time.Now(),
	}
	m.statusCache, _ = lru.NewWithEvict(512, m.onCacheEvict)
	if strings.HasSuffix(m.logDirectory, "/") {
		m.logDirectory = strings.TrimSuffix(m.logDirectory, "/")
	}
	m.onStartup()
	return m
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

	allocations := s.Topology.findServers(dc, in.GetComputeResources())

	log.Printf("%v requests %+v, allocated %+v", in.FlowHashCode, in.GetComputeResources(), allocations)

	return &pb.AllocationResult{
		Allocations: allocations,
	}, nil

}

func (s *MasterServer) SendHeartbeat(stream pb.GleamMaster_SendHeartbeatServer) error {
	var location *pb.Location
	for {
		heartbeat, err := stream.Recv()
		if err == nil {
			if location == nil {
				location = heartbeat.Location
				log.Printf("added agent: %v", location)
			}
		} else {
			if location != nil {
				s.Topology.deleteAgentInformation(location)
			}
			log.Printf("lost agent: %v", location)

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

func (s *MasterServer) SendFlowExecutionStatus(stream pb.GleamMaster_SendFlowExecutionStatusServer) error {
	for {
		status, err := stream.Recv()

		if err == io.EOF {
			return nil
		}

		if err != nil {
			return err
		}

		s.statusCache.Add(status.GetId(), status)

		if status.Driver.GetStopTime() != 0 {
			data, _ := proto.Marshal(status)
			ioutil.WriteFile(fmt.Sprintf("%s/f%d.log", s.logDirectory, status.GetId()), data, 0644)
		}
	}
}

func (s *MasterServer) onStartup() {
	files, _ := filepath.Glob(fmt.Sprintf("%s/f[0-9]*\\.log", s.logDirectory))
	for _, f := range files {
		data, _ := ioutil.ReadFile(f)
		status := &pb.FlowExecutionStatus{}
		if err := proto.Unmarshal(data, status); err == nil {
			// println("loading", f, "for", status.GetId())
			s.statusCache.Add(status.GetId(), status)
		} else {
			os.Remove(f)
		}
	}
}

func (s *MasterServer) onCacheEvict(key interface{}, value interface{}) {
	id := key.(uint32)
	os.Remove(fmt.Sprintf("%s/f%d.log", s.logDirectory, id))
}
