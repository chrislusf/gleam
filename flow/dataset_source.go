package flow

import (
	"bufio"
	"io"
	"log"
	"os"

	"github.com/chrislusf/gleam/util"
)

// Source produces data feeding into the flow.
// Function f writes to this writer.
// The written bytes should be MsgPack encoded []byte.
// Use util.EncodeRow(...) to encode the data before sending to this channel
func (fc *FlowContext) Source(f func(io.Writer)) (ret *Dataset) {
	ret = fc.newNextDataset(1)
	step := fc.AddOneToOneStep(nil, ret)
	step.IsOnDriverSide = true
	step.Name = "Source"
	step.Function = func(task *Task) {
		// println("running source task...")
		for _, shard := range task.OutputShards {
			f(shard.IncomingChan.Writer)
			shard.IncomingChan.Writer.Close()
		}
	}
	return
}

// TextFile reads the file content as lines and feed into the flow.
func (fc *FlowContext) TextFile(fname string) (ret *Dataset) {
	fn := func(out io.Writer) {
		file, err := os.Open(fname)
		if err != nil {
			log.Panicf("Can not open file %s: %v", fname, err)
			return
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			util.WriteRow(out, scanner.Bytes())
		}

		if err := scanner.Err(); err != nil {
			log.Printf("Scan file %s: %v", fname, err)
		}
	}
	return fc.Source(fn)
}

// Channel accepts a channel to feed into the flow.
func (fc *FlowContext) Channel(ch chan interface{}) (ret *Dataset) {
	ret = fc.newNextDataset(1)
	step := fc.AddOneToOneStep(nil, ret)
	step.IsOnDriverSide = true
	step.Name = "Channel"
	step.Function = func(task *Task) {
		for data := range ch {
			util.WriteRow(task.OutputShards[0].IncomingChan.Writer, data)
			task.OutputShards[0].Counter++
		}
		for _, shard := range task.OutputShards {
			shard.IncomingChan.Writer.Close()
		}
	}
	return
}

// Bytes begins a flow with an [][]byte
func (fc *FlowContext) Bytes(slice [][]byte) (ret *Dataset) {
	inputChannel := make(chan interface{})

	go func() {
		for _, data := range slice {
			inputChannel <- data
		}
		close(inputChannel)
	}()

	return fc.Channel(inputChannel)
}

// Strings begins a flow with an []string
func (fc *FlowContext) Strings(lines []string) (ret *Dataset) {
	inputChannel := make(chan interface{})

	go func() {
		for _, data := range lines {
			inputChannel <- []byte(data)
		}
		close(inputChannel)
	}()

	return fc.Channel(inputChannel)
}

// Ints begins a flow with an []int
func (fc *FlowContext) Ints(numbers []int) (ret *Dataset) {
	inputChannel := make(chan interface{})

	go func() {
		for _, data := range numbers {
			inputChannel <- data
		}
		close(inputChannel)
	}()

	return fc.Channel(inputChannel)
}
