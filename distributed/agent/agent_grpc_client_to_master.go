package agent

import (
	"fmt"
	"log"
	"time"

	"github.com/chrislusf/gleam/pb"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

func (as *AgentServer) heartbeat() {

	for {
		err := as.doHeartbeat(10 * time.Second)
		if err != nil {
			time.Sleep(30 * time.Second)
		}
	}

}

func (as *AgentServer) doHeartbeat(sleepInterval time.Duration) error {

	grpcConnection, err := grpc.Dial(as.Master, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("fail to dial: %v", err)
	}
	defer grpcConnection.Close()

	client := pb.NewGleamMasterClient(grpcConnection)

	stream, err := client.SendHeartbeat(context.Background())
	if err != nil {
		log.Printf("SendHeartbeat error: %v", err)
		return err
	}

	log.Printf("Heartbeat to %s", as.Master)

	ticker := time.NewTicker(sleepInterval)
	quickTicker := time.NewTicker(500 * time.Millisecond)
	for {
		select {
		case <-quickTicker.C:
			if as.allocatedHasChanges {
				as.allocatedHasChanges = false
				if err := as.sendOneHeartbeat(stream); err != nil {
					return err
				}
			}
		case <-ticker.C:
			if err := as.sendOneHeartbeat(stream); err != nil {
				return err
			}
		}
	}

}

func (as *AgentServer) sendOneHeartbeat(stream pb.GleamMaster_SendHeartbeatClient) error {
	beat := &pb.Heartbeat{
		Location: &pb.Location{
			DataCenter: *as.Option.DataCenter,
			Rack:       *as.Option.Rack,
			Server:     *as.Option.Host,
			Port:       int32(*as.Option.Port),
		},
		Resource:  as.computeResource,
		Allocated: as.allocatedResource,
	}

	// log.Printf("Reporting allocated %v", as.allocatedResource)

	if err := stream.Send(beat); err != nil {
		log.Printf("%v.Send(%v) = %v", stream, beat, err)
		return err
	}
	return nil
}
