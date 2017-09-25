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

	err := withClient(exe.Option.AgentAddress, func(client pb.GleamAgentClient) error {
		stream, err := client.CollectExecutionStatistics(context.Background(), grpc.FailFast(false))
		if err != nil {
			return fmt.Errorf("executor => agent %v : %v", exe.Option.AgentAddress, err)
		}

		tickChan := time.Tick(1 * time.Second)
		stat := &pb.ExecutionStat{
			FlowHashCode: exe.instructions.FlowHashCode,
			Stats:        exe.stats,
		}
		for {
			select {
			case <-tickChan:
				if err := stream.Send(stat); err != nil {
					return fmt.Errorf("executor Send(%v): %v", exe.stats, err)
				}
			case <-finishedChan:
				stream.CloseSend()
				return nil
			}
		}

	})

	if err != nil {
		log.Printf("executor heartbeat to agent %v: %v", exe.Option.AgentAddress, err)
	}

}

func (exe *Executor) reportStatus() {

	err := withClient(exe.Option.AgentAddress, func(client pb.GleamAgentClient) error {
		stream, err := client.CollectExecutionStatistics(context.Background(), grpc.FailFast(false))
		if err != nil {
			return fmt.Errorf("executor => agent %v : %v", exe.Option.AgentAddress, err)
		}
		// defer stream.CloseSend()

		stat := &pb.ExecutionStat{
			FlowHashCode: exe.instructions.FlowHashCode,
			Stats:        exe.stats,
		}

		if err := stream.Send(stat); err != nil {
			return fmt.Errorf("%v.Send(%v) = %v", stream, exe.stats, err)
		}

		return nil
	})

	if err != nil {
		log.Printf("executor reportStatus to %v: %v", exe.Option.AgentAddress, err)
	}

}

func withClient(server string, fn func(client pb.GleamAgentClient) error) error {
	grpcConnection, err := grpc.Dial(server,
		grpc.WithInsecure(),
		grpc.WithBlock(),
	)
	if err != nil {
		return fmt.Errorf("executor dial agent: %v", err)
	}
	defer func() {
		time.Sleep(50 * time.Millisecond)
		grpcConnection.Close()
	}()
	client := pb.NewGleamAgentClient(grpcConnection)

	return fn(client)
}
