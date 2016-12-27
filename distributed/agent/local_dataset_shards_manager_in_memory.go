package agent

import (
	"io"
	"sync"
	"time"

	"github.com/chrislusf/gleam/util"
)

type trackedChannel struct {
	incomingChannel  *util.Piper
	outgoingChannels []*util.Piper
	index            int
	wg               *sync.WaitGroup
	lastWriteAt      time.Time
	isClosed         bool
}

func newTrackedChannel(readerCount int) *trackedChannel {
	var wg sync.WaitGroup
	t := &trackedChannel{
		incomingChannel:  util.NewPiper(),
		outgoingChannels: make([]*util.Piper, readerCount),
		index:            0,
		wg:               &wg,
		lastWriteAt:      time.Now(),
	}
	if readerCount > 1 {
		for i := range t.outgoingChannels {
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
			t.lastWriteAt = time.Now()
			t.isClosed = true
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
	if tc, ok := m.name2Channel[name]; ok {
		delete(m.name2Channel, name)
		tc.incomingChannel.Writer.Close()
	}
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
			// println("found existing channel", name, "closed:", tc.isClosed)
			if tc.isClosed {
				return nil
			}
			return tc.borrowChannel()
		}
		// println("waiting for", name, m, m.name2Channel[name])
		m.name2ChannelCond.Wait()
		// println("woke up for", name, m, m.name2Channel[name])
	}

}

func (m *LocalDatasetShardsManagerInMemory) Cleanup(name string) {

	m.Lock()
	defer m.Unlock()

	m.doDelete(name)
}

// purge executor status older than 24 hours to save memory
func (m *LocalDatasetShardsManagerInMemory) purgeExpiredEntries() {
	for {
		func() {
			m.Lock()
			cutoverLimit := time.Now().Add(-24 * time.Hour)
			var oldShardNames []string
			for name, tc := range m.name2Channel {
				if tc.lastWriteAt.Before(cutoverLimit) {
					oldShardNames = append(oldShardNames, name)
				}
			}
			for _, name := range oldShardNames {
				m.doDelete(name)
			}
			m.Unlock()
			time.Sleep(1 * time.Hour)
		}()
	}
}
