package flow

import (
	"bufio"
	"log"
	"os"
)

// Inputs: f(chan A), shardCount
func (fc *FlowContext) Source(f func(chan []byte)) (ret *Dataset) {
	ret = fc.newNextDataset(1)
	step := fc.AddOneToOneStep(nil, ret)
	step.Function = func(task *Task) {
		println("running source task...")
		for _, shard := range task.Outputs {
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
			out <- scanner.Bytes()
		}

		if err := scanner.Err(); err != nil {
			log.Printf("Scan file %s: %v", fname, err)
		}
	}
	return fc.Source(fn)
}
