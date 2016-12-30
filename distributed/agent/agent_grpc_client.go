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

	grpcConection, err := grpc.Dial(as.Master, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("fail to dial: %v", err)
	}
	defer grpcConection.Close()

	client := pb.NewGleamMasterClient(grpcConection)

	stream, err := client.SendHeartbeat(context.Background())
	if err != nil {
		log.Printf("%v.SendHeartbeat(_) = _, %v", client, err)
		return err
	}

	log.Printf("Heartbeat to %s", as.Master)

	for {
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
		if err := stream.Send(beat); err != nil {
			log.Printf("%v.Send(%v) = %v", stream, beat, err)
			return err
		}
		time.Sleep(sleepInterval)
	}

}
