package agent

import (
	"fmt"
	"sync"

	"github.com/chrislusf/glow/netchan/store"
)

type LocalDatasetShardsManager struct {
	sync.Mutex
	dir            string
	port           int
	name2Store     map[string]store.DataStore
	name2StoreCond *sync.Cond
}

func NewLocalDatasetShardsManager(dir string, port int) *LocalDatasetShardsManager {
	m := &LocalDatasetShardsManager{
		dir:        dir,
		port:       port,
		name2Store: make(map[string]store.DataStore),
	}
	m.name2StoreCond = sync.NewCond(m)
	return m
}

func (m *LocalDatasetShardsManager) doDelete(name string) {

	ds, ok := m.name2Store[name]
	if !ok {
		return
	}

	delete(m.name2Store, name)

	ds.Destroy()
}

func (m *LocalDatasetShardsManager) DeleteNamedDatasetShard(name string) {

	m.Lock()
	defer m.Unlock()

	m.doDelete(name)

}

func (m *LocalDatasetShardsManager) CreateNamedDatasetShard(name string) store.DataStore {

	m.Lock()
	defer m.Unlock()

	_, ok := m.name2Store[name]
	if ok {
		m.doDelete(name)
	}

	s := store.NewLocalFileDataStore(m.dir, fmt.Sprintf("%s-%d", name, m.port))

	m.name2Store[name] = s
	// println(name, "is broadcasting...")
	m.name2StoreCond.Broadcast()

	return s

}

func (m *LocalDatasetShardsManager) WaitForNamedDatasetShard(name string) store.DataStore {

	m.Lock()
	defer m.Unlock()

	for {
		if ds, ok := m.name2Store[name]; ok {
			return ds
		}
		// println(name, "is waiting to read...")
		m.name2StoreCond.Wait()
	}

	return nil

}
