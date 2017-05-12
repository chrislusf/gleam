package executor

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/chrislusf/gleam/pb"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

func (exe *Executor) statusHeartbeat(finishedChan chan bool) {

	fmt.Fprintf(os.Stderr, "instructions: %v, stats: %+v\n", exe.instructions.InstructionNames(), exe.stats[len(exe.stats)-1])

	grpcConection, err := grpc.Dial(exe.Option.AgentAddress, grpc.WithInsecure())
	if err != nil {
		log.Printf("fail to dial: %v", err)
		return
	}
	defer grpcConection.Close()

	client := pb.NewGleamAgentClient(grpcConection)

	stream, err := client.CollectExecutionStatistics(context.Background())
	if err != nil {
		log.Printf("%v.CollectExecutionStatistics(_) = _, %v", client, err)
		return
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
				return
			}
		case <-finishedChan:
			if err := stream.Send(stat); err != nil {
				log.Printf("%v.Send(%v) = %v", stream, exe.stats, err)
				return
			}
			return
		}
	}

}

func (exe *Executor) reportStatus() {

	grpcConection, err := grpc.Dial(exe.Option.AgentAddress, grpc.WithInsecure())
	if err != nil {
		log.Printf("fail to dial: %v", err)
		return
	}
	defer grpcConection.Close()

	client := pb.NewGleamAgentClient(grpcConection)

	stream, err := client.CollectExecutionStatistics(context.Background())
	if err != nil {
		log.Printf("%v.CollectExecutionStatistics(_) = _, %v", client, err)
		return
	}

	stat := &pb.ExecutionStat{
		FlowHashCode: exe.instructions.FlowHashCode,
		Name:         exe.instructions.Name,
		Stats:        exe.stats,
	}

	if err := stream.Send(stat); err != nil {
		log.Printf("%v.Send(%v) = %v", stream, exe.stats, err)
		return
	}

}
