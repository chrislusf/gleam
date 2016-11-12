package adapter

import (
	"sync"
)

var (
	AdapterManager = &adatperManager{
		adapters: make(map[string]Adapter),
	}
	ConnectionManager = &connectionManager{
		connections: make(map[string]*ConnectionInfo),
	}
)

type ConnectionInfo struct {
	sync.RWMutex
	Adapter Adapter
	config  map[string]string
}

func RegisterAdapter(a Adapter) {
	AdapterManager.Lock()
	defer AdapterManager.Unlock()
	AdapterManager.adapters[a.AdapterName()] = a
}

func (am *adatperManager) GetAdapter(name string) (Adapter, bool) {
	am.RLock()
	defer am.RUnlock()
	a, ok := am.adapters[name]
	return a, ok
}

func RegisterConnection(id string, adapterName string) *ConnectionInfo {
	ConnectionManager.Lock()
	defer ConnectionManager.Unlock()
	AdapterManager.RLock()
	defer AdapterManager.RUnlock()

	ci := &ConnectionInfo{
		Adapter: AdapterManager.adapters[adapterName],
		config:  make(map[string]string),
	}
	ConnectionManager.connections[id] = ci
	return ci
}

type adatperManager struct {
	sync.RWMutex
	adapters map[string]Adapter
}

type connectionManager struct {
	sync.RWMutex
	connections map[string]*ConnectionInfo
}

func (ci *ConnectionInfo) Set(name, value string) *ConnectionInfo {
	ci.Lock()
	defer ci.Unlock()
	ci.config[name] = value
	return ci
}

func (cm *connectionManager) GetConnectionInfo(name string) (*ConnectionInfo, bool) {
	cm.RLock()
	defer cm.RUnlock()
	c, ok := cm.connections[name]
	return c, ok
}

func (ci *ConnectionInfo) GetConfig() map[string]string {
	ci.RLock()
	defer ci.RUnlock()
	ret := make(map[string]string)
	for k, v := range ci.config {
		ret[k] = v
	}
	return ret
}
