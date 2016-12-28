package scheduler

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/chrislusf/gleam/pb"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

func sendExecutionRequest(server string, request *pb.ExecutionRequest) error {
	grpcConection, err := grpc.Dial(server, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("fail to dial: %v", err)
	}
	client := pb.NewGleamAgentClient(grpcConection)

	stream, err := client.Execute(context.Background(), request)
	if err != nil {
		log.Printf("%v.Execute(_) = _, %v", client, err)
		return err
	}
	for {
		response, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("%v.Execute(_) = _, %v", client, err)
			break
		}
		if response.GetError() != nil {
			return errors.New(server + ">" + string(response.GetError()))
		}
		if response.GetOutput() != nil {
			fmt.Fprintf(os.Stdout, "%s>%s\n", server, string(response.GetOutput()))
		}
		if response.GetSystemTime() != 0 {
			log.Printf("%s %v> SystemTime: %2.2fs UserTime: %2.2fs\n", server, request.GetInstructions().InstructionNames(), response.GetSystemTime(), response.GetUserTime())
		}
	}

	return err
}

func sendDeleteRequest(server string, request *pb.DeleteDatasetShardRequest) error {
	grpcConection, err := grpc.Dial(server, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("fail to dial: %v", err)
	}
	client := pb.NewGleamAgentClient(grpcConection)

	_, err = client.Delete(context.Background(), request)
	if err != nil {
		log.Printf("%v.Delete(_) = _, %v", client, err)
	}
	return err
}
