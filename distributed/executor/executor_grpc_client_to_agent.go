package executor

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/chrislusf/gleam/pb"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

func (exe *Executor) statusHeartbeat(wg *sync.WaitGroup, finishedChan chan bool) {

	defer wg.Done()

	withClient(exe.Option.AgentAddress, func(client pb.GleamAgentClient) error {
		stream, err := client.CollectExecutionStatistics(context.Background())
		if err != nil {
			log.Printf("%v.CollectExecutionStatistics(_) = _, %v", client, err)
			return nil
		}

		tickChan := time.Tick(1 * time.Second)
		stat := &pb.ExecutionStat{
			FlowHashCode: exe.instructions.FlowHashCode,
			Name:         exe.instructions.Name,
			Stats:        exe.stats,
		}
		for {
			select {
			case <-tickChan:
				if err := stream.Send(stat); err != nil {
					log.Printf("%v.Send(%v) = %v", stream, exe.stats, err)
					return nil
				}
			case <-finishedChan:
				stream.CloseSend()
				return nil
			}
		}

	})

}

func (exe *Executor) reportStatus() {

	withClient(exe.Option.AgentAddress, func(client pb.GleamAgentClient) error {
		stream, err := client.CollectExecutionStatistics(context.Background())
		if err != nil {
			log.Printf("%v.CollectExecutionStatistics(_) = _, %v", client, err)
			return nil
		}
		// defer stream.CloseSend()

		stat := &pb.ExecutionStat{
			FlowHashCode: exe.instructions.FlowHashCode,
			Name:         exe.instructions.Name,
			Stats:        exe.stats,
		}

		if err := stream.Send(stat); err != nil {
			log.Printf("%v.Send(%v) = %v", stream, exe.stats, err)
			return nil
		}

		return nil
	})

}

func withClient(server string, fn func(client pb.GleamAgentClient) error) error {
	grpcConection, err := grpc.Dial(server, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("executor dial agent: %v", err)
	}
	defer func() {
		time.Sleep(50 * time.Millisecond)
		grpcConection.Close()
	}()
	client := pb.NewGleamAgentClient(grpcConection)

	return fn(client)
}
