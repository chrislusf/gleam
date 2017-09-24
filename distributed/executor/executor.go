package executor

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/chrislusf/gleam/distributed/netchan"
	"github.com/chrislusf/gleam/instruction"
	"github.com/chrislusf/gleam/pb"
	"github.com/chrislusf/gleam/util"
)

type ExecutorOption struct {
	AgentAddress string
	HashCode     uint32
}

type Executor struct {
	Option       *ExecutorOption
	instructions *pb.InstructionSet
	stats        []*pb.InstructionStat
	grpcAddress  string
}

func NewExecutor(option *ExecutorOption, instructions *pb.InstructionSet) *Executor {

	return &Executor{
		Option:       option,
		instructions: instructions,
	}
}

func (exe *Executor) ExecuteInstructionSet() error {

	// start a listener for stats
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	exe.grpcAddress = listener.Addr().String()
	go exe.serveGrpc(listener)

	//TODO pass in the context
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	exeErrChan := make(chan error, len(exe.instructions.GetInstructions()))
	ioErrChan := make(chan error, 2*len(exe.instructions.GetInstructions()))
	finishedChan := make(chan bool, 1)

	prevIsPipe := false
	prevOutputChan := util.NewPiper()
	for index, instr := range exe.instructions.GetInstructions() {
		inputChan := prevOutputChan
		outputChan := util.NewPiper()
		wg.Add(1)
		stat := &pb.InstructionStat{
			StepId: instr.StepId,
			TaskId: instr.TaskId,
		}
		exe.stats = append(exe.stats, stat)
		go func(index int, instr *pb.Instruction, prevIsPipe bool, inChan, outChan *util.Piper, stats *pb.InstructionStat) {
			exe.executeInstruction(ctx, &wg, ioErrChan, exeErrChan, inChan, outChan,
				prevIsPipe,
				exe.instructions,
				instr,
				index == 0,
				index == len(exe.instructions.GetInstructions())-1,
				int(exe.instructions.GetReaderCount()),
				stat,
			)
		}(index, instr, prevIsPipe, inputChan, outputChan, stat)
		prevOutputChan = outputChan
		if instr.GetScript() != nil {
			prevIsPipe = instr.GetScript().GetIsPipe()
		} else {
			prevIsPipe = false
		}
	}

	var heartbeatWg sync.WaitGroup
	heartbeatWg.Add(1)
	go exe.statusHeartbeat(&heartbeatWg, finishedChan)
	defer heartbeatWg.Wait()

	go func() {
		wg.Wait()
		close(finishedChan)
	}()

	select {
	case <-finishedChan:
		exe.reportStatus()
	case err := <-ioErrChan:
		if err != nil {
			cancel()
			return err
		}
	case err := <-exeErrChan:
		if err != nil {
			cancel()
			return err
		}
	}

	return nil
}

func setupReaders(ctx context.Context, wg *sync.WaitGroup, ioErrChan chan error,
	i *pb.Instruction, inPiper *util.Piper, isFirst bool) (readers []io.Reader) {

	if !isFirst {
		readers = append(readers, inPiper.Reader)
	} else {
		for _, inputLocation := range i.GetInputShardLocations() {
			wg.Add(1)
			inChan := util.NewPiper()
			// println(i.GetName(), "connecting to", inputLocation.Address(), "to read", inputLocation.GetName())
			go func(inputLocation *pb.DatasetShardLocation) {
				err := netchan.DialReadChannel(ctx, wg, i.GetName(), inputLocation.Address(), inputLocation.GetName(), inputLocation.GetOnDisk(), inChan.Writer)
				if err != nil {
					ioErrChan <- fmt.Errorf("Failed %s reading %s from %s: %v", i.GetName(), inputLocation.GetName(), inputLocation.Address(), err)
				}
			}(inputLocation)
			readers = append(readers, inChan.Reader)
		}
	}
	return
}
func setupWriters(ctx context.Context, wg *sync.WaitGroup, ioErrChan chan error,
	i *pb.Instruction, outPiper *util.Piper, isLast bool, readerCount int) (writers []io.Writer) {

	if !isLast {
		writers = append(writers, outPiper.Writer)
	} else {
		for _, outputLocation := range i.GetOutputShardLocations() {
			wg.Add(1)
			outChan := util.NewPiper()
			// println(i.GetName(), "connecting to", outputLocation.Address(), "to write", outputLocation.GetName(), "readerCount", readerCount)
			go func(outputLocation *pb.DatasetShardLocation) {
				err := netchan.DialWriteChannel(ctx, wg, i.GetName(), outputLocation.Address(), outputLocation.GetName(), outputLocation.GetOnDisk(), outChan.Reader, readerCount)
				if err != nil {
					ioErrChan <- fmt.Errorf("Failed %s writing %s to %s: %v", i.GetName(), outputLocation.GetName(), outputLocation.Address(), err)
				}
			}(outputLocation)
			writers = append(writers, outChan.Writer)
		}
	}
	return
}

func (exe *Executor) executeInstruction(ctx context.Context, wg *sync.WaitGroup,
	ioErrChan, exeErrChan chan error,
	inChan, outChan *util.Piper, prevIsPipe bool,
	is *pb.InstructionSet, i *pb.Instruction,
	isFirst, isLast bool, readerCount int, stat *pb.InstructionStat) {

	defer wg.Done()

	readers := setupReaders(ctx, wg, ioErrChan, i, inChan, isFirst)
	writers := setupWriters(ctx, wg, ioErrChan, i, outChan, isLast, readerCount)

	defer func() {
		for _, writer := range writers {
			if c, ok := writer.(io.Closer); ok {
				c.Close()
			}
		}
	}()

	util.BufWrites(writers, func(writers []io.Writer) {
		if f := instruction.InstructionRunner.GetInstructionFunction(i); f != nil {
			if prevIsPipe {
				var tmpReaders []io.Reader
				for _, r := range readers {
					tmpReaders = append(tmpReaders, util.ConvertLineReaderToRowReader(r, "pipeToRow", os.Stderr))
				}
				readers = tmpReaders
			}
			err := f(readers, writers, stat)
			if err != nil {
				// println(i.GetName(), "running error", err.Error())
				exeErrChan <- fmt.Errorf("Failed executing function %s: %v", i.GetName(), err)
			}
			return
		}

		//TODO add errChan to scripts also?

		var err error
		// println("starting", i.Name, "inChan", inChan, "outChan", outChan)
		if !i.GetScript().IsPipe {
			i.GetScript().Args[len(i.GetScript().Args)-1] = fmt.Sprintf(
				"%s -gleam.executor=%s -flow.hashcode=%d -flow.stepId=%d -flow.taskId=%d -gleam.profiling=%v",
				i.GetScript().Args[len(i.GetScript().Args)-1],
				exe.grpcAddress,
				is.FlowHashCode,
				i.StepId,
				i.TaskId,
				is.IsProfiling,
			)
		}

		// println("args:", i.GetScript().Args[len(i.GetScript().Args)-1])

		if i.GetScript() != nil {
			for x := 0; x < 3; x++ {
				command := exec.CommandContext(ctx,
					i.GetScript().GetPath(), i.GetScript().GetArgs()...,
				)
				// fmt.Fprintf(os.Stderr, "starting %d %d: %v\n", i.StepId, i.TaskId, command.Args)
				wg.Add(1)
				err = util.Execute(ctx, wg, stat, i.GetName(), command, readers[0], writers[0], prevIsPipe, i.GetScript().GetIsPipe(), false, os.Stderr)
				if err == nil || stat.InputCounter != 0 {
					break
				}
				if err != nil {
					log.Printf("Failed %d time to start %v %v %v:%v", (x + 1), command.Path, command.Args, command.Env, err)
					time.Sleep(time.Duration(1) * time.Second)
				}
			}
			if err != nil {
				exeErrChan <- fmt.Errorf("Failed executing command %s: %v", i.GetName(), err)
			}
		} else {
			panic("what is this? " + i.String())
		}

	})

}
