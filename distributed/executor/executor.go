package executor

import (
	"os"
	"os/exec"
	"sync"

	"github.com/chrislusf/gleam/distributed/cmd"
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
	if i.GetScript() != nil {
		command := exec.Command(
			i.GetScript().GetPath(), i.GetScript().GetArgs()...,
		)
		util.Execute(wg, i.GetScript().GetName(), command, inChan, outChan, i.GetScript().GetIsPipe(), true, os.Stderr)
	} else if i.GetLocalSort() != nil {
		flow.LocalSort(inChan, outChan)
		close(outChan)
	} else if i.GetMergeSortedTo() != nil {
		inChans := make([]chan []byte)
		// TODO: read from the dataset shard locations
		flow.MergeSortedTo(inChans, outChan)
		close(outChan)
	} else if i.GetScatterPartitions() != nil {
	} else if i.GetCollectPartitions() != nil {
	} else if i.GetJoinPartitionedSorted() != nil {
	} else if i.GetCoGroupPartitionedSorted() != nil {
	}
}
