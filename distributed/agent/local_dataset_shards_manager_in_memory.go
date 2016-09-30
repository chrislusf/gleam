package agent

import (
	"sync"
)

type LocalDatasetShardsManagerInMemory struct {
	sync.Mutex
	name2Channel     map[string]chan []byte
	name2ChannelCond *sync.Cond
}

func NewLocalDatasetShardsManagerInMemory() *LocalDatasetShardsManagerInMemory {
	m := &LocalDatasetShardsManagerInMemory{
		name2Channel: make(map[string]chan []byte),
	}
	m.name2ChannelCond = sync.NewCond(m)
	return m
}

func (m *LocalDatasetShardsManagerInMemory) doDelete(name string) {

	ch, ok := m.name2Channel[name]
	if !ok {
		return
	}

	delete(m.name2Channel, name)

	close(ch)
}

func (m *LocalDatasetShardsManagerInMemory) DeleteNamedDatasetShard(name string) {

	m.Lock()
	defer m.Unlock()

	m.doDelete(name)

}

func (m *LocalDatasetShardsManagerInMemory) CreateNamedDatasetShard(name string) chan []byte {

	m.Lock()
	defer m.Unlock()

	_, ok := m.name2Channel[name]
	if ok {
		m.doDelete(name)
	}

	ch := make(chan []byte, 1024)

	m.name2Channel[name] = ch
	println(name, "is broadcasting...")
	m.name2ChannelCond.Broadcast()

	return ch

}

func (m *LocalDatasetShardsManagerInMemory) WaitForNamedDatasetShard(name string) chan []byte {

	m.Lock()
	defer m.Unlock()

	for {
		if ch, ok := m.name2Channel[name]; ok {
			return ch
		}
		println(name, "is waiting to read...")
		m.name2ChannelCond.Wait()
	}

	return nil

}
