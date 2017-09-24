package scheduler

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/chrislusf/gleam/distributed/resource"
	"github.com/chrislusf/gleam/pb"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

func sendRelatedFile(ctx context.Context, client pb.GleamAgentClient, flowHashCode uint32, relatedFile resource.FileResource) error {
	fh, err := resource.GenerateFileHash(relatedFile.FullPath)
	if err != nil {
		log.Printf("Failed2 to read %s: %v", relatedFile.FullPath, err)
		return err
	}

	fileResourceRequest := &pb.FileResourceRequest{
		Name:         filepath.Base(relatedFile.FullPath),
		Dir:          relatedFile.TargetFolder,
		Hash:         fh.Hash,
		FlowHashCode: flowHashCode,
	}

	stream, err := client.SendFileResource(ctx, grpc.FailFast(false))
	if err != nil {
		log.Printf("%v.SendFileResource(_) = _, %v", client, err)
		return err
	}

	err = stream.Send(fileResourceRequest)
	if err != nil {
		log.Printf("%v.SendFirstFileResource(_) = _, %v", client, err)
		return err
	}

	fileResourceResponse, err := stream.Recv()
	if err != nil {
		log.Printf("%v.CheckFileResourceExists(_) = _, %v", client, err)
		return err
	}

	if fileResourceResponse.AlreadyExists {
		stream.CloseSend()
		return nil
	}

	f, err := os.Open(relatedFile.FullPath)
	if err != nil {
		log.Printf("OpenFile %s error: %v", relatedFile.FullPath, err)
		stream.CloseSend()
		return err
	}
	defer f.Close()

	buffer := make([]byte, 4*1024)
	for {
		n, err := f.Read(buffer)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("File Read %s error: %v", relatedFile.FullPath, err)
			return err
		}
		fileResource := &pb.FileResourceRequest{
			Name:         filepath.Base(relatedFile.FullPath),
			Dir:          relatedFile.TargetFolder,
			Content:      buffer[0:n],
			FlowHashCode: flowHashCode,
		}
		err = stream.Send(fileResource)
		if err != nil {
			log.Printf("%v.Send file %s: %v", client, fileResource.Name, err)
			return err
		}
	}

	stream.CloseSend()

	// receive ack
	stream.Recv()

	return nil

}

func sendExecutionRequest(ctx context.Context,
	_ *pb.FlowExecutionStatus_TaskGroup,
	executionStatus *pb.FlowExecutionStatus_TaskGroup_Execution,
	server string, request *pb.ExecutionRequest) error {

	return withClient(server, func(client pb.GleamAgentClient) error {
		log.Printf("%s %v> starting with %v MB memory...\n", server, request.InstructionSet.Name, request.GetResource().GetMemoryMb())
		stream, err := client.Execute(ctx, request, grpc.FailFast(false))
		if err != nil {
			log.Printf("sendExecutionRequest.Execute: %v", err)
			return err
		}

		// stream.CloseSend()

		for {
			response, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Printf("sendExecutionRequest %v stream from %s: %v", request.GetInstructionSet().GetName(), server, err)
				break
			}
			if response.GetError() != nil {
				log.Printf("%s %v>%s", server, request.InstructionSet.Name, string(response.GetError()))
				executionStatus.Error = response.GetError()
			}
			if response.GetOutput() != nil {
				fmt.Fprintf(os.Stdout, "%s>%s\n", server, string(response.GetOutput()))
			}
			if response.GetSystemTime() != 0 {
				// log.Printf("%s %v>  UserTime: %2.2fs SystemTime: %2.2fs\n", server, request.InstructionSet.Name, response.GetSystemTime(), response.GetUserTime())
				executionStatus.SystemTime = response.GetSystemTime()
				executionStatus.UserTime = response.GetUserTime()
			}
			if response.GetExecutionStat() != nil {
				if executionStatus.ExecutionStat == nil {
					executionStatus.ExecutionStat = response.GetExecutionStat()
				} else {
					executionStatus.ExecutionStat.Stats = mergeStats(
						executionStatus.ExecutionStat.Stats,
						response.GetExecutionStat().GetStats())
				}
			}
		}

		return err

	})
}

// merge existing stats with incoming stats
func mergeStats(a, b []*pb.InstructionStat) (ret []*pb.InstructionStat) {
	var nonOverlapping []*pb.InstructionStat
	for _, ai := range a {
		var found bool
		for _, bi := range b {
			if ai.StepId == bi.StepId {
				found = true
				if ai.InputCounter > bi.InputCounter {
					ret = append(ret, ai)
				} else {
					ret = append(ret, bi)
				}
			}
		}
		if !found {
			nonOverlapping = append(nonOverlapping, ai)
		}
	}
	for _, bi := range b {
		var found bool
		for _, ai := range a {
			if ai.StepId == bi.StepId {
				found = true
			}
		}
		if !found {
			nonOverlapping = append(nonOverlapping, bi)
		}
	}
	ret = append(ret, nonOverlapping...)
	return ret
}

func sendDeleteRequest(server string, request *pb.DeleteDatasetShardRequest) error {
	return withClient(server, func(client pb.GleamAgentClient) error {
		_, err := client.Delete(context.Background(), request, grpc.FailFast(false))
		if err != nil {
			log.Printf("%v.Delete(_) = _, %v", client, err)
		}
		return err
	})
}

func SendCleanupRequest(server string, request *pb.CleanupRequest) error {
	return withClient(server, func(client pb.GleamAgentClient) error {
		_, err := client.Cleanup(context.Background(), request, grpc.FailFast(false))
		if err != nil {
			log.Printf("%v.Delete(_) = _, %v", client, err)
		}
		return err
	})
}

func withClient(server string, fn func(client pb.GleamAgentClient) error) error {
	grpcConnection, err := grpc.Dial(server,
		grpc.WithInsecure(),
		grpc.WithBlock(),
	)
	if err != nil {
		return fmt.Errorf("driver dial agent: %v", err)
	}
	defer func() {
		time.Sleep(50 * time.Millisecond)
		grpcConnection.Close()
	}()
	client := pb.NewGleamAgentClient(grpcConnection)

	return fn(client)
}
