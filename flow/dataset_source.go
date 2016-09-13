package flow

import (
	"bufio"
	"log"
	"os"

	"github.com/chrislusf/gleam/util"
)

func (fc *FlowContext) Source(f func(chan []byte)) (ret *Dataset) {
	ret = fc.newNextDataset(1)
	step := fc.AddOneToOneStep(nil, ret)
	step.Name = "Source"
	step.Function = func(task *Task) {
		// println("running source task...")
		for _, shard := range task.OutputShards {
			f(shard.IncomingChan)
			close(shard.IncomingChan)
		}
	}
	return
}

func (fc *FlowContext) TextFile(fname string) (ret *Dataset) {
	fn := func(out chan []byte) {
		file, err := os.Open(fname)
		if err != nil {
			log.Panicf("Can not open file %s: %v", fname, err)
			return
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			encoded, err := util.EncodeRow(scanner.Bytes())
			if err != nil {
				log.Printf("Failed to encode bytes: %v", err)
				continue
			}
			out <- encoded
		}

		if err := scanner.Err(); err != nil {
			log.Printf("Scan file %s: %v", fname, err)
		}
	}
	return fc.Source(fn)
}

func (fc *FlowContext) Channel(ch chan []byte) (ret *Dataset) {
	ret = fc.newNextDataset(1)
	step := fc.AddOneToOneStep(nil, ret)
	step.Name = "Channel"
	step.Function = func(task *Task) {
		for data := range ch {
			encoded, err := util.EncodeRow(data)
			if err != nil {
				log.Printf("Failed to encode bytes: %v", err)
				continue
			}
			task.OutputShards[0].IncomingChan <- encoded
		}
		for _, shard := range task.OutputShards {
			close(shard.IncomingChan)
		}
	}
	return
}

func (fc *FlowContext) Slice(slice [][]byte) (ret *Dataset) {
	inputChannel := make(chan []byte)

	go func() {
		for _, data := range slice {
			inputChannel <- data
		}
		close(inputChannel)
	}()

	return fc.Channel(inputChannel)
}

func (fc *FlowContext) Lines(lines []string) (ret *Dataset) {
	inputChannel := make(chan []byte)

	go func() {
		for _, data := range lines {
			inputChannel <- []byte(data)
		}
		close(inputChannel)
	}()

	return fc.Channel(inputChannel)
}
