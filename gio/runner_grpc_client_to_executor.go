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

	err := withClient(runner.Option.ExecutorAddress, func(client pb.GleamExecutorClient) error {
		stream, err := client.CollectExecutionStatistics(context.Background())
		if err != nil {
			return fmt.Errorf("runner => executor %v: %v", runner.Option.ExecutorAddress, err)
		}

		tickChan := time.Tick(1 * time.Second)

		for {
			select {
			case <-tickChan:
				if err := stream.Send(stat); err != nil {
					return fmt.Errorf("runner Send(%v): %v", stat, err)
				}
			case <-finishedChan:
				stream.CloseSend()
				return nil
			}
		}

	})

	if err != nil {
		log.Printf("runner heartbeat to %v: %v", runner.Option.ExecutorAddress, err)
	}

}

func (runner *gleamRunner) reportStatus() {

	err := withClient(runner.Option.ExecutorAddress, func(client pb.GleamExecutorClient) error {
		stream, err := client.CollectExecutionStatistics(context.Background())
		if err != nil {
			return fmt.Errorf("runner => executor %v: %v", runner.Option.ExecutorAddress, err)
		}
		// defer stream.CloseSend()

		if err := stream.Send(stat); err != nil {
			log.Printf("%v.Send(%v) = %v", stream, stat, err)
			return nil
		}

		return nil
	})

	if err != nil {
		log.Printf("runner reportStatus to %v: %v", runner.Option.ExecutorAddress, err)
	}

}

func withClient(server string, fn func(client pb.GleamExecutorClient) error) error {
	if server == "" {
		return nil
	}

	grpcConection, err := grpc.Dial(server, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("executor dial agent: %v", err)
	}
	/**  Could be closed prematurely before fn finish its use of the connection
	defer func() {
		time.Sleep(50 * time.Millisecond)
		grpcConection.Close()
	}()
	*/
	client := pb.NewGleamExecutorClient(grpcConection)
	err = fn(client)
	defer grpcConection.Close()  // fn has finished its use of the client, connection could be closed safely
	return err
}
