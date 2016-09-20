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

func (exe *Executor) ExecuteInstructionSet(finalOutputChan chan []byte) {
	var wg sync.WaitGroup

	var outputChan chan []byte
	for index, instruction := range exe.instructions.GetInstructions() {
		inputChan := outputChan
		if index == len(exe.instructions.GetInstructions())-1 {
			outputChan = finalOutputChan
		} else {
			outputChan = make(chan []byte, 16)
		}
		wg.Add(1)
		go exe.ExecuteInstruction(&wg, inputChan, outputChan, instruction)
	}

	wg.Wait()
}

func (exe *Executor) ExecuteInstruction(wg *sync.WaitGroup, inChan, outChan chan []byte, i *cmd.Instruction) {
	defer wg.Done()
	defer close(outChan)

	if i.GetScript() != nil {

		command := exec.Command(
			i.GetScript().GetPath(), i.GetScript().GetArgs()...,
		)
		util.Execute(wg, i.GetScript().GetName(), command, inChan, outChan, i.GetScript().GetIsPipe(), false, os.Stderr)

	} else if i.GetLocalSort() != nil {

		flow.LocalSort(inChan, outChan)

	} else if i.GetMergeSortedTo() != nil {

		var inChans []chan []byte
		for _, inputLocation := range i.GetMergeSortedTo().GetInputShardLocations() {
			wg.Add(1)
			var inChan chan []byte
			go netchan.DialReadChannel(wg, inputLocation.Address(), inputLocation.GetShard().Topic(), inChan)
			inChans = append(inChans, inChan)
		}
		flow.MergeSortedTo(inChans, outChan)

	} else if i.GetScatterPartitions() != nil {

		var outChans []chan []byte
		for _, outputLocation := range i.GetScatterPartitions().GetOutputShardLocations() {
			wg.Add(1)
			var outChan chan []byte
			go netchan.DialWriteChannel(wg, outputLocation.Address(), outputLocation.GetShard().Topic(), outChan)
			outChans = append(outChans, outChan)
		}
		flow.ScatterPartitions(inChan, outChans)
		for _, outChan := range outChans {
			close(outChan)
		}

	} else if i.GetCollectPartitions() != nil {

		var inChans []chan []byte
		for _, inputLocation := range i.GetCollectPartitions().GetInputShardLocations() {
			wg.Add(1)
			var inChan chan []byte
			go netchan.DialReadChannel(wg, inputLocation.Address(), inputLocation.GetShard().Topic(), inChan)
			inChans = append(inChans, inChan)
		}
		flow.CollectPartitions(inChans, outChan)

	} else if i.GetJoinPartitionedSorted() != nil {

		var leftChan, rightChan chan []byte
		jps := i.GetJoinPartitionedSorted()
		leftLocation := jps.GetLeftInputShardLocation()
		rightLocation := jps.GetRightInputShardLocation()
		wg.Add(1)
		go netchan.DialReadChannel(wg, leftLocation.Address(), leftLocation.GetShard().Topic(), leftChan)
		wg.Add(1)
		go netchan.DialReadChannel(wg, rightLocation.Address(), rightLocation.GetShard().Topic(), rightChan)

		flow.JoinPartitionedSorted(leftChan, rightChan, *jps.IsLeftOuterJoin, *jps.IsRightOuterJoin, outChan)

	} else if i.GetCoGroupPartitionedSorted() != nil {

		var leftChan, rightChan chan []byte
		jps := i.GetCoGroupPartitionedSorted()
		leftLocation := jps.GetLeftInputShardLocation()
		rightLocation := jps.GetRightInputShardLocation()
		wg.Add(1)
		go netchan.DialReadChannel(wg, leftLocation.Address(), leftLocation.GetShard().Topic(), leftChan)
		wg.Add(1)
		go netchan.DialReadChannel(wg, rightLocation.Address(), rightLocation.GetShard().Topic(), rightChan)

		flow.CoGroupPartitionedSorted(leftChan, rightChan, outChan)

	}
}
