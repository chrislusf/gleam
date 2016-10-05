package executor

import (
	"io"
	"os"
	"os/exec"
	"sync"

	"github.com/chrislusf/gleam/distributed/cmd"
	"github.com/chrislusf/gleam/distributed/netchan"
	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/util"
)

type ExecutorOption struct {
	Master       *string
	Host         *string
	Port         *int
	Dir          *string
	DataCenter   *string
	Rack         *string
	MaxExecutor  *int
	MemoryMB     *int64
	CPULevel     *int
	CleanRestart *bool
	HashCode     *uint32
}

type Executor struct {
	Option       *ExecutorOption
	Master       string
	instructions *cmd.InstructionSet
}

func NewExecutor(option *ExecutorOption, instructions *cmd.InstructionSet) *Executor {

	return &Executor{
		Option:       option,
		instructions: instructions,
	}
}

func (exe *Executor) ExecuteInstructionSet() {
	var wg sync.WaitGroup

	prevIsPipe := false
	prevOutputChan := util.NewPiper()
	for index, instruction := range exe.instructions.GetInstructions() {
		inputChan := prevOutputChan
		outputChan := util.NewPiper()
		wg.Add(1)
		go func(index int, instruction *cmd.Instruction, prevIsPipe bool, inChan, outChan *util.Piper) {
			exe.ExecuteInstruction(&wg, inChan, outChan,
				prevIsPipe,
				instruction,
				index == 0,
				index == len(exe.instructions.GetInstructions())-1,
				int(exe.instructions.GetReaderCount()),
			)
		}(index, instruction, prevIsPipe, inputChan, outputChan)
		prevOutputChan = outputChan
		if instruction.GetScript() != nil {
			prevIsPipe = instruction.GetScript().GetIsPipe()
		} else {
			prevIsPipe = false
		}
	}

	wg.Wait()
}

func connectInputOutput(wg *sync.WaitGroup, executorName string, inChan, outChan *util.Piper, inLocation, outLocation *cmd.DatasetShardLocation, isFirst, isLast bool, readerCount int) {
	if isFirst && inChan != nil {
		wg.Add(1)
		go netchan.DialReadChannel(wg, executorName, inLocation.Address(), inLocation.GetShard().Name(), inChan.Writer)
	}
	if isLast && outChan != nil {
		wg.Add(1)
		go netchan.DialWriteChannel(wg, executorName, outLocation.Address(), outLocation.GetShard().Name(), outChan.Reader, readerCount)
	}
}

// TODO: refactor this
func (exe *Executor) ExecuteInstruction(wg *sync.WaitGroup, inChan, outChan *util.Piper, prevIsPipe bool, i *cmd.Instruction, isFirst, isLast bool, readerCount int) {
	defer wg.Done()
	if outChan != nil {
		defer outChan.Writer.Close()
		// defer println("closing", *i.Name, "outChan", outChan)
	}

	// println("starting", *i.Name, "inChan", inChan, "outChan", outChan)
	if i.GetScript() != nil {
		connectInputOutput(wg, i.GetName(), inChan, outChan, i.GetScript().GetInputShardLocation(), i.GetScript().GetOutputShardLocation(), isFirst, isLast, readerCount)

		command := exec.Command(
			i.GetScript().GetPath(), i.GetScript().GetArgs()...,
		)
		wg.Add(1)
		util.Execute(wg, i.GetName(), command, inChan, outChan, prevIsPipe, i.GetScript().GetIsPipe(), false, os.Stderr)

	} else if i.GetLocalSort() != nil {

		connectInputOutput(wg, i.GetName(), inChan, outChan, i.GetLocalSort().GetInputShardLocation(), i.GetLocalSort().GetOutputShardLocation(), isFirst, isLast, readerCount)

		flow.LocalSort(inChan.Reader, outChan.Writer)

	} else if i.GetPipeAsArgs() != nil {

		connectInputOutput(wg, i.GetName(), inChan, outChan, i.GetPipeAsArgs().GetInputShardLocation(), i.GetPipeAsArgs().GetOutputShardLocation(), isFirst, isLast, readerCount)

		flow.PipeAsArgs(inChan, i.GetPipeAsArgs().GetCode(), outChan)

	} else if i.GetMergeSortedTo() != nil {

		var inChans []io.Reader
		for _, inputLocation := range i.GetMergeSortedTo().GetInputShardLocations() {
			wg.Add(1)
			inChan := util.NewPiper()
			go netchan.DialReadChannel(wg, i.GetName(), inputLocation.Address(), inputLocation.GetShard().Name(), inChan.Writer)
			inChans = append(inChans, inChan.Reader)
		}
		connectInputOutput(wg, i.GetName(), nil, outChan, nil, i.GetMergeSortedTo().GetOutputShardLocation(), isFirst, isLast, readerCount)
		flow.MergeSortedTo(inChans, outChan.Writer)

	} else if i.GetScatterPartitions() != nil {

		var outChans []*util.Piper
		for _, outputLocation := range i.GetScatterPartitions().GetOutputShardLocations() {
			wg.Add(1)
			outChan := util.NewPiper()
			go netchan.DialWriteChannel(wg, i.GetName(), outputLocation.Address(), outputLocation.GetShard().Name(), outChan.Reader, 1)
			outChans = append(outChans, outChan)
		}
		connectInputOutput(wg, i.GetName(), inChan, nil, i.GetScatterPartitions().GetInputShardLocation(), nil, isFirst, isLast, readerCount)
		var writers []io.Writer
		for _, outChan := range outChans {
			writers = append(writers, outChan.Writer)
		}
		flow.ScatterPartitions(inChan.Reader, writers)
		for _, outChan := range outChans {
			outChan.Writer.Close()
		}

	} else if i.GetCollectPartitions() != nil {

		var inChans []io.Reader
		for _, inputLocation := range i.GetCollectPartitions().GetInputShardLocations() {
			wg.Add(1)
			inChan := util.NewPiper()
			go netchan.DialReadChannel(wg, i.GetName(), inputLocation.Address(), inputLocation.GetShard().Name(), inChan.Writer)
			inChans = append(inChans, inChan.Reader)
		}
		connectInputOutput(wg, i.GetName(), nil, outChan, nil, i.GetCollectPartitions().GetOutputShardLocation(), isFirst, isLast, readerCount)
		flow.CollectPartitions(inChans, outChan.Writer)

	} else if i.GetJoinPartitionedSorted() != nil {

		leftChan, rightChan := util.NewPiper(), util.NewPiper()
		jps := i.GetJoinPartitionedSorted()
		leftLocation := jps.GetLeftInputShardLocation()
		rightLocation := jps.GetRightInputShardLocation()
		wg.Add(1)
		go netchan.DialReadChannel(wg, i.GetName(), leftLocation.Address(), leftLocation.GetShard().Name(), leftChan.Writer)
		wg.Add(1)
		go netchan.DialReadChannel(wg, i.GetName(), rightLocation.Address(), rightLocation.GetShard().Name(), rightChan.Writer)

		connectInputOutput(wg, i.GetName(), nil, outChan, nil, i.GetJoinPartitionedSorted().GetOutputShardLocation(), isFirst, isLast, readerCount)
		flow.JoinPartitionedSorted(leftChan.Reader, rightChan.Reader, *jps.IsLeftOuterJoin, *jps.IsRightOuterJoin, outChan.Writer)

	} else if i.GetCoGroupPartitionedSorted() != nil {

		leftChan, rightChan := util.NewPiper(), util.NewPiper()
		jps := i.GetCoGroupPartitionedSorted()
		leftLocation := jps.GetLeftInputShardLocation()
		rightLocation := jps.GetRightInputShardLocation()
		wg.Add(1)
		go netchan.DialReadChannel(wg, i.GetName(), leftLocation.Address(), leftLocation.GetShard().Name(), leftChan.Writer)
		wg.Add(1)
		go netchan.DialReadChannel(wg, i.GetName(), rightLocation.Address(), rightLocation.GetShard().Name(), rightChan.Writer)

		connectInputOutput(wg, i.GetName(), nil, outChan, nil, i.GetCoGroupPartitionedSorted().GetOutputShardLocation(), isFirst, isLast, readerCount)
		flow.CoGroupPartitionedSorted(leftChan.Reader, rightChan.Reader, outChan.Writer)

	} else {
		panic("what is this? " + i.String())
	}
}
