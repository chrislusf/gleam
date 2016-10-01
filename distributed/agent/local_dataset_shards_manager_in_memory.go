package agent

import (
	"sync"
)

type trackedChannel struct {
	writerChannel  chan []byte
	readerChannels []chan []byte
	index          int
}

func newTrackedChannel(readerCount int) *trackedChannel {
	t := &trackedChannel{
		writerChannel:  make(chan []byte),
		readerChannels: make([]chan []byte, readerCount),
		index:          0,
	}
	if readerCount > 1 {
		for i, _ := range t.readerChannels {
			t.readerChannels[i] = make(chan []byte)
		}
		go func() {
			for data := range t.writerChannel {
				for _, ch := range t.readerChannels {
					ch <- data
				}
			}
			for _, ch := range t.readerChannels {
				close(ch)
			}
		}()
	}
	return t
}

func (tc *trackedChannel) borrowChannel() chan []byte {
	if len(tc.readerChannels) > 1 {
		tc.index++
		return tc.readerChannels[tc.index-1]
	}
	return tc.writerChannel
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

	delete(m.name2Channel, name)

}

func (m *LocalDatasetShardsManagerInMemory) CreateNamedDatasetShard(name string, readerCount int) chan []byte {

	m.Lock()
	defer m.Unlock()

	_, ok := m.name2Channel[name]
	if ok {
		m.doDelete(name)
	}

	tc := newTrackedChannel(readerCount)

	m.name2Channel[name] = tc
	m.name2ChannelCond.Broadcast()

	return tc.writerChannel
}

func (m *LocalDatasetShardsManagerInMemory) WaitForNamedDatasetShard(name string) chan []byte {

	m.Lock()
	defer m.Unlock()

	for {
		if tc, ok := m.name2Channel[name]; ok {
			return tc.borrowChannel()
		}
		m.name2ChannelCond.Wait()
	}

	return nil

}

func (m *LocalDatasetShardsManagerInMemory) Cleanup(name string) {

	m.Lock()
	defer m.Unlock()

	m.doDelete(name)
}
