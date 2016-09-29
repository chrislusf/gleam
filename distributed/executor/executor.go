package executor

import (
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

	prevOutputChan := make(chan []byte, 16)
	var outputChan chan []byte
	for index, instruction := range exe.instructions.GetInstructions() {
		inputChan := prevOutputChan
		outputChan = make(chan []byte, 16)
		wg.Add(1)
		go exe.ExecuteInstruction(&wg, inputChan, outputChan, instruction, index == 0, index == len(exe.instructions.GetInstructions())-1)
		prevOutputChan = outputChan
	}

	wg.Wait()
}

func connectInputOutput(wg *sync.WaitGroup, inChan, outChan chan []byte, inLocation, outLocation *cmd.DatasetShardLocation, isFirst, isLast bool) {
	if isFirst && inChan != nil {
		wg.Add(1)
		go netchan.DialReadChannel(wg, inLocation.Address(), inLocation.GetShard().Name(), inChan)
	}
	if isLast && outChan != nil {
		wg.Add(1)
		go netchan.DialWriteChannel(wg, outLocation.Address(), outLocation.GetShard().Name(), outChan)
	}
}

// TODO: refactor this
func (exe *Executor) ExecuteInstruction(wg *sync.WaitGroup, inChan, outChan chan []byte, i *cmd.Instruction, isFirst, isLast bool) {
	defer wg.Done()
	if outChan != nil {
		defer close(outChan)
	}

	if i.GetScript() != nil {
		connectInputOutput(wg, inChan, outChan, i.GetScript().GetInputShardLocation(), i.GetScript().GetOutputShardLocation(), isFirst, isLast)

		command := exec.Command(
			i.GetScript().GetPath(), i.GetScript().GetArgs()...,
		)
		wg.Add(1)
		util.Execute(wg, i.GetScript().GetName(), command, inChan, outChan, i.GetScript().GetIsPipe(), false, os.Stderr)

	} else if i.GetLocalSort() != nil {

		connectInputOutput(wg, inChan, outChan, i.GetLocalSort().GetInputShardLocation(), i.GetLocalSort().GetOutputShardLocation(), isFirst, isLast)

		flow.LocalSort(inChan, outChan)

	} else if i.GetPipeAsArgs() != nil {

		connectInputOutput(wg, inChan, outChan, i.GetPipeAsArgs().GetInputShardLocation(), i.GetPipeAsArgs().GetOutputShardLocation(), isFirst, isLast)

		flow.PipeAsArgs(inChan, i.GetPipeAsArgs().GetCode(), outChan)

	} else if i.GetMergeSortedTo() != nil {

		var inChans []chan []byte
		for _, inputLocation := range i.GetMergeSortedTo().GetInputShardLocations() {
			wg.Add(1)
			inChan := make(chan []byte, 16)
			go netchan.DialReadChannel(wg, inputLocation.Address(), inputLocation.GetShard().Name(), inChan)
			inChans = append(inChans, inChan)
		}
		connectInputOutput(wg, nil, outChan, nil, i.GetMergeSortedTo().GetOutputShardLocation(), isFirst, isLast)
		flow.MergeSortedTo(inChans, outChan)

	} else if i.GetScatterPartitions() != nil {

		var outChans []chan []byte
		for _, outputLocation := range i.GetScatterPartitions().GetOutputShardLocations() {
			wg.Add(1)
			outChan := make(chan []byte, 16)
			go netchan.DialWriteChannel(wg, outputLocation.Address(), outputLocation.GetShard().Name(), outChan)
			outChans = append(outChans, outChan)
		}
		connectInputOutput(wg, inChan, nil, i.GetScatterPartitions().GetInputShardLocation(), nil, isFirst, isLast)
		flow.ScatterPartitions(inChan, outChans)
		for _, outChan := range outChans {
			close(outChan)
		}

	} else if i.GetCollectPartitions() != nil {

		var inChans []chan []byte
		for _, inputLocation := range i.GetCollectPartitions().GetInputShardLocations() {
			wg.Add(1)
			inChan := make(chan []byte, 16)
			go netchan.DialReadChannel(wg, inputLocation.Address(), inputLocation.GetShard().Name(), inChan)
			inChans = append(inChans, inChan)
		}
		connectInputOutput(wg, nil, outChan, nil, i.GetCollectPartitions().GetOutputShardLocation(), isFirst, isLast)
		flow.CollectPartitions(inChans, outChan)

	} else if i.GetJoinPartitionedSorted() != nil {

		leftChan, rightChan := make(chan []byte, 16), make(chan []byte, 16)
		jps := i.GetJoinPartitionedSorted()
		leftLocation := jps.GetLeftInputShardLocation()
		rightLocation := jps.GetRightInputShardLocation()
		wg.Add(1)
		go netchan.DialReadChannel(wg, leftLocation.Address(), leftLocation.GetShard().Name(), leftChan)
		wg.Add(1)
		go netchan.DialReadChannel(wg, rightLocation.Address(), rightLocation.GetShard().Name(), rightChan)

		connectInputOutput(wg, nil, outChan, nil, i.GetJoinPartitionedSorted().GetOutputShardLocation(), isFirst, isLast)
		flow.JoinPartitionedSorted(leftChan, rightChan, *jps.IsLeftOuterJoin, *jps.IsRightOuterJoin, outChan)

	} else if i.GetCoGroupPartitionedSorted() != nil {

		leftChan, rightChan := make(chan []byte, 16), make(chan []byte, 16)
		jps := i.GetCoGroupPartitionedSorted()
		leftLocation := jps.GetLeftInputShardLocation()
		rightLocation := jps.GetRightInputShardLocation()
		wg.Add(1)
		go netchan.DialReadChannel(wg, leftLocation.Address(), leftLocation.GetShard().Name(), leftChan)
		wg.Add(1)
		go netchan.DialReadChannel(wg, rightLocation.Address(), rightLocation.GetShard().Name(), rightChan)

		connectInputOutput(wg, nil, outChan, nil, i.GetCoGroupPartitionedSorted().GetOutputShardLocation(), isFirst, isLast)
		flow.CoGroupPartitionedSorted(leftChan, rightChan, outChan)

	} else {
		panic("what is this? " + i.String())
	}
}
