package gio

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/chrislusf/gleam/pb"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

func (runner *gleamRunner) statusHeartbeat(wg *sync.WaitGroup, finishedChan chan bool) {

	defer wg.Done()

	withClient(runner.Option.AgentAddress, func(client pb.GleamAgentClient) error {
		stream, err := client.CollectExecutionStatistics(context.Background())
		if err != nil {
			log.Printf("%v.CollectExecutionStatistics(_) = _, %v", client, err)
			return nil
		}

		tickChan := time.Tick(1 * time.Second)

		for {
			select {
			case <-tickChan:
				if err := stream.Send(stat); err != nil {
					log.Printf("%v.Send(%v) = %v", stream, stat, err)
					return nil
				}
			case <-finishedChan:
				stream.CloseSend()
				return nil
			}
		}

	})

}

func (runner *gleamRunner) reportStatus() {

	withClient(runner.Option.AgentAddress, func(client pb.GleamAgentClient) error {
		stream, err := client.CollectExecutionStatistics(context.Background())
		if err != nil {
			log.Printf("%v.CollectExecutionStatistics(_) = _, %v", client, err)
			return nil
		}
		// defer stream.CloseSend()

		if err := stream.Send(stat); err != nil {
			log.Printf("%v.Send(%v) = %v", stream, stat, err)
			return nil
		}

		println(fmt.Sprintf("%v", stat))

		return nil
	})

}

func withClient(server string, fn func(client pb.GleamAgentClient) error) error {
	if server == "" {
		return nil
	}

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
