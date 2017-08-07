package executor

import (
	"io"
	"net"

	"github.com/chrislusf/gleam/pb"
	"google.golang.org/grpc"
)

func (exe *Executor) serveGrpc(listener net.Listener) {

	grpcServer := grpc.NewServer()
	pb.RegisterGleamExecutorServer(grpcServer, exe)
	grpcServer.Serve(listener)
}

// Collect stat from "gleam execute" started mapper reducer process
func (exe *Executor) CollectExecutionStatistics(stream pb.GleamExecutor_CollectExecutionStatisticsServer) error {

	for {
		stats, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		for _, stat := range stats.Stats {
			for i, current := range exe.stats {
				if current.StepId == stat.StepId && current.TaskId == stat.TaskId {
					exe.stats[i] = stat
					// fmt.Printf("executor received stat: %+v\n", stat)
					break
				}
			}
		}
	}

}
