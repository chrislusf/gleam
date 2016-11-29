package executor

import (
	//"bufio"
	"io"
	"os"
	"os/exec"
	"sync"

	"github.com/chrislusf/gleam/distributed/netchan"
	"github.com/chrislusf/gleam/instruction"
	"github.com/chrislusf/gleam/msg"
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
	instructions *msg.InstructionSet
}

func NewExecutor(option *ExecutorOption, instructions *msg.InstructionSet) *Executor {

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
		go func(index int, instruction *msg.Instruction, prevIsPipe bool, inChan, outChan *util.Piper) {
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

func setupReaders(wg *sync.WaitGroup, i *msg.Instruction, inPiper *util.Piper, isFirst bool) (readers []io.Reader) {
	if !isFirst {
		readers = append(readers, inPiper.Reader)
	} else {
		for _, inputLocation := range i.GetInputShardLocations() {
			wg.Add(1)
			inChan := util.NewPiper()
			// println(i.GetName(), "connecting to", inputLocation.Address(), "to read", inputLocation.GetName())
			go netchan.DialReadChannel(wg, i.GetName(), inputLocation.Address(), inputLocation.GetName(), i.GetOnDisk(), inChan.Writer)
			readers = append(readers, inChan.Reader)
		}
	}
	return
}
func setupWriters(wg *sync.WaitGroup, i *msg.Instruction, outPiper *util.Piper, isLast bool, readerCount int) (writers []io.Writer) {
	if !isLast {
		writers = append(writers, outPiper.Writer)
	} else {
		for _, outputLocation := range i.GetOutputShardLocations() {
			wg.Add(1)
			outChan := util.NewPiper()
			// println(i.GetName(), "connecting to", outputLocation.Address(), "to write", outputLocation.GetName(), "readerCount", readerCount)
			go netchan.DialWriteChannel(wg, i.GetName(), outputLocation.Address(), outputLocation.GetName(), i.GetOnDisk(), outChan.Reader, readerCount)
			writers = append(writers, outChan.Writer)
		}
	}
	return
}

func (exe *Executor) ExecuteInstruction(wg *sync.WaitGroup, inChan, outChan *util.Piper, prevIsPipe bool, i *msg.Instruction, isFirst, isLast bool, readerCount int) {
	defer wg.Done()

	readers := setupReaders(wg, i, inChan, isFirst)
	writers := setupWriters(wg, i, outChan, isLast, readerCount)

	defer func() {
		for _, writer := range writers {
			if c, ok := writer.(io.Closer); ok {
				c.Close()
			}
		}
	}()

	util.BufWrites(writers, func(writers []io.Writer) {
		if f := instruction.InstructionRunner.GetInstructionFunction(i); f != nil {
			//TODO use the stats
			stats := &instruction.Stats{}
			f(readers, writers, stats)
			return
		}

		// println("starting", *i.Name, "inChan", inChan, "outChan", outChan)
		if i.GetScript() != nil {
			command := exec.Command(
				i.GetScript().GetPath(), i.GetScript().GetArgs()...,
			)
			wg.Add(1)
			util.Execute(wg, i.GetName(), command, readers[0], writers[0], prevIsPipe, i.GetScript().GetIsPipe(), false, os.Stderr)

		} else {
			panic("what is this? " + i.String())
		}

	})

}
