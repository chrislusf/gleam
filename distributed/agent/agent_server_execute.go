package agent

import (
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"sync"

	"github.com/chrislusf/gleam/pb"
	"github.com/golang/protobuf/proto"
	"github.com/kardianos/osext"
)

func (as *AgentServer) executeCommand(
	stream pb.GleamAgent_ExecuteServer,
	startRequest *pb.ExecutionRequest,
	dir string,
	statChan chan *pb.ExecutionStat,
) (err error) {

	ctx := stream.Context()
	stopChan := make(chan bool)

	// start the command
	executableFullFilename, _ := osext.Executable()
	command := exec.CommandContext(
		ctx,
		executableFullFilename,
		"execute",
		"--note",
		startRequest.GetInstructionSet().GetName(),
	)
	stdin, err := command.StdinPipe()
	if err != nil {
		log.Printf("Failed to create stdin pipe: %v", err)
		return
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		log.Printf("Failed to create stdout pipe: %v", err)
		return
	}
	stderr, err := command.StderrPipe()
	if err != nil {
		log.Printf("Failed to create stderr pipe: %v", err)
		return
	}
	// msg.Env = startRequest.Envs
	command.Dir = dir

	if err = command.Start(); err != nil {
		log.Printf("Failed to start command %s under %s: %v",
			command.Path, command.Dir, err)
		return err
	}

	errors := make([]error, 2)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		errors[0] = streamOutput(&wg, stream, stdout)
	}()
	wg.Add(1)
	go func() {
		errors[1] = streamError(&wg, stream, stderr)
	}()
	wg.Add(1)
	go streamPulse(&wg, stopChan, statChan, stream)

	// send instruction set to executor
	msgMessageBytes, err := proto.Marshal(startRequest.GetInstructionSet())
	if err != nil {
		log.Printf("Failed to marshal command %s: %v",
			startRequest.GetInstructionSet().String(), err)
		return err
	}
	if _, err = stdin.Write(msgMessageBytes); err != nil {
		log.Printf("Failed to write command: %v", err)
		return err
	}
	if err = stdin.Close(); err != nil {
		log.Printf("Failed to close command: %v", err)
		return err
	}

	// wait for finish
	waitErr := command.Wait()
	if waitErr != nil {
		log.Printf("Failed to run command %s: %v", startRequest.GetInstructionSet().GetName(), waitErr)
	}

	stopChan <- true
	wg.Wait()

	sendExitStats(stream, command)

	if waitErr != nil {
		return waitErr
	}
	if errors[0] != nil {
		return errors[0]
	}
	if errors[1] != nil {
		return errors[1]
	}

	return nil

}

func streamOutput(wg *sync.WaitGroup, stream pb.GleamAgent_ExecuteServer, reader io.Reader) error {

	defer wg.Done()

	buffer := make([]byte, 1024)
	for {
		n, err := reader.Read(buffer)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("Failed to read stdout: %v", err)
		}
		if n == 0 {
			continue
		}

		if sendErr := stream.Send(&pb.ExecutionResponse{
			Output: buffer[0:n],
		}); sendErr != nil {
			return fmt.Errorf("Failed to send stdout: %v", sendErr)
		}
	}
}

func streamError(wg *sync.WaitGroup, stream pb.GleamAgent_ExecuteServer, reader io.Reader) error {

	defer wg.Done()

	tee := io.TeeReader(reader, os.Stderr)

	buffer := make([]byte, 1024)
	for {
		n, err := tee.Read(buffer)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("Failed to read stderr: %v", err)
		}
		if n == 0 {
			continue
		}

		if sendErr := stream.Send(&pb.ExecutionResponse{
			Error: buffer[0:n],
		}); sendErr != nil {
			return fmt.Errorf("Failed to send stderr: %v", sendErr)
		}
	}
}

func streamPulse(wg *sync.WaitGroup,
	stopChan chan bool,
	statChan chan *pb.ExecutionStat,
	stream pb.GleamAgent_ExecuteServer) error {

	defer wg.Done()

	for {
		select {
		case <-stopChan:
			return nil
		case stat := <-statChan:
			if sendErr := stream.Send(&pb.ExecutionResponse{
				ExecutionStat: stat,
			}); sendErr != nil {
				return fmt.Errorf("Failed to send empty response: %v\n", sendErr)
			}
		}
	}
}

func sendExitStats(stream pb.GleamAgent_ExecuteServer, cmd *exec.Cmd) error {
	if cmd.ProcessState != nil {
		if sendErr := stream.Send(&pb.ExecutionResponse{
			SystemTime: cmd.ProcessState.SystemTime().Seconds(),
			UserTime:   cmd.ProcessState.UserTime().Seconds(),
		}); sendErr != nil {
			return fmt.Errorf("Failed to send exit stats response: %v\n", sendErr)
		}
	}
	return nil
}
