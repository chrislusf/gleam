package flow

import (
	"github.com/chrislusf/gleam/util"
)

func (step *Step) NewTask() (task *Task) {
	task = &Task{Step: step, Id: len(step.Tasks)}
	step.Tasks = append(step.Tasks, task)
	return
}

func (t *Task) MergedInputChan() chan []byte {
	if len(t.InputShards) == 1 && len(t.InputShards[0].OutgoingChans) == 1 {
		return t.InputShards[0].OutgoingChans[0]
	}
	var prevChans []chan []byte
	for _, shard := range t.InputShards {
		prevChans = append(prevChans, shard.OutgoingChans...)
	}

	inputChan := make(chan []byte)
	util.MergeChannel(prevChans, inputChan)
	return inputChan
}
