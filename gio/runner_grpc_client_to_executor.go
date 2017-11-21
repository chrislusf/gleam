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
		stream, err := client.CollectExecutionStatistics(context.Background(), grpc.FailFast(false))
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
		stream, err := client.CollectExecutionStatistics(context.Background(), grpc.FailFast(false))
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

	// using block option, a dial may never return even if the server is inaccessible, so
	// a more appropriate way is pairing with DialContext and setting a timeout value
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	grpcConnection, err := grpc.DialContext(ctx, server,
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
	client := pb.NewGleamExecutorClient(grpcConnection)

	return fn(client)
}
