package scheduler

import (
	"fmt"
	"io"
	"log"
	"os"

	"github.com/chrislusf/gleam/pb"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

func sendExecutionRequest(ctx context.Context,
	statusExecution *pb.FlowExecutionStatus_TaskGroup_Execution,
	server string, request *pb.ExecutionRequest) error {
	grpcConection, err := grpc.Dial(server, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("fail to dial: %v", err)
	}
	defer grpcConection.Close()
	client := pb.NewGleamAgentClient(grpcConection)

	log.Printf("%s %v> starting with %v MB memory...\n", server, request.Name, request.GetResource().GetMemoryMb())
	stream, err := client.Execute(ctx, request)
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
			break
		}
		if response.GetError() != nil {
			err = fmt.Errorf("%s %v> %v", server, request.Name, string(response.GetError()))
			return err
		}
		if response.GetOutput() != nil {
			fmt.Fprintf(os.Stdout, "%s>%s\n", server, string(response.GetOutput()))
		}
		if response.GetSystemTime() != 0 {
			log.Printf("%s %v>  UserTime: %2.2fs SystemTime: %2.2fs\n", server, request.Name, response.GetSystemTime(), response.GetUserTime())
			statusExecution.SystemTime = response.GetSystemTime()
			statusExecution.UserTime = response.GetUserTime()
		}
	}

	return err
}

func sendDeleteRequest(server string, request *pb.DeleteDatasetShardRequest) error {
	grpcConection, err := grpc.Dial(server, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("fail to dial: %v", err)
	}
	defer grpcConection.Close()
	client := pb.NewGleamAgentClient(grpcConection)

	_, err = client.Delete(context.Background(), request)
	if err != nil {
		log.Printf("%v.Delete(_) = _, %v", client, err)
	}
	return err
}
