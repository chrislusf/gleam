package agent

import (
	"io"
	"sync"

	"github.com/chrislusf/gleam/util"
)

type trackedChannel struct {
	incomingChannel  *util.Piper
	outgoingChannels []*util.Piper
	index            int
	wg               *sync.WaitGroup
}

func newTrackedChannel(readerCount int) *trackedChannel {
	var wg sync.WaitGroup
	t := &trackedChannel{
		incomingChannel:  util.NewPiper(),
		outgoingChannels: make([]*util.Piper, readerCount),
		index:            0,
		wg:               &wg,
	}
	if readerCount > 1 {
		for i, _ := range t.outgoingChannels {
			t.outgoingChannels[i] = util.NewPiper()
		}
		t.wg.Add(1)
		go func() {
			defer t.wg.Done()
			var writers []io.Writer
			for _, outgoingChan := range t.outgoingChannels {
				writers = append(writers, outgoingChan.Writer)
			}
			w := io.MultiWriter(writers...)
			io.Copy(w, t.incomingChannel.Reader)
			for _, outgoingChan := range t.outgoingChannels {
				outgoingChan.Writer.Close()
			}
		}()
	}
	return t
}

func (tc *trackedChannel) borrowChannel() *util.Piper {
	if len(tc.outgoingChannels) > 1 {
		tc.index++
		return tc.outgoingChannels[tc.index-1]
	}
	return tc.incomingChannel
}

type LocalDatasetShardsManagerInMemory struct {
	sync.Mutex
	name2Channel     map[string]*trackedChannel
	name2ChannelCond *sync.Cond
}

func NewLocalDatasetShardsManagerInMemory() *LocalDatasetShardsManagerInMemory {
	m := &LocalDatasetShardsManagerInMemory{
		name2Channel: make(map[string]*trackedChannel),
	}
	m.name2ChannelCond = sync.NewCond(m)
	return m
}

func (m *LocalDatasetShardsManagerInMemory) doDelete(name string) {

	// println("deleting", name, "from", m, m.name2Channel[name])
	delete(m.name2Channel, name)

}

func (m *LocalDatasetShardsManagerInMemory) CreateNamedDatasetShard(name string, readerCount int) *trackedChannel {

	m.Lock()
	defer m.Unlock()

	_, ok := m.name2Channel[name]
	if ok {
		m.doDelete(name)
	}

	tc := newTrackedChannel(readerCount)

	m.name2Channel[name] = tc
	m.name2ChannelCond.Broadcast()
	// println("setting", name, "to", m, m.name2Channel[name])

	return tc
}

func (m *LocalDatasetShardsManagerInMemory) WaitForNamedDatasetShard(name string) *util.Piper {

	m.Lock()
	defer m.Unlock()

	for {
		if tc, ok := m.name2Channel[name]; ok {
			return tc.borrowChannel()
		}
		// println("waiting for", name, m, m.name2Channel[name])
		m.name2ChannelCond.Wait()
		// println("woke up for", name, m, m.name2Channel[name])
	}

	return nil

}

func (m *LocalDatasetShardsManagerInMemory) Cleanup(name string) {

	m.Lock()
	defer m.Unlock()

	m.doDelete(name)
}
