package adapter

import (
	"fmt"
	"sync"

	"github.com/spf13/viper"
)

var (
	AdapterManager = &adatperManager{
		adapters: make(map[string]func() Adapter),
	}
	ConnectionManager = &connectionManager{
		connections: make(map[string]*ConnectionInfo),
	}
)

func init() {
	viper.SetConfigName("gleam")        // name of config file (without extension)
	viper.AddConfigPath("/etc/gleam/")  // path to look for the config file in
	viper.AddConfigPath("$HOME/.gleam") // call multiple times to add many search paths
	viper.AddConfigPath(".")            // optionally look for config in the working directory
	err := viper.ReadInConfig()         // Find and read the config file
	if err != nil {                     // Handle errors reading the config file
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			// skip this
			println("no gleam.yaml found.")
		} else {
			panic(fmt.Errorf("Fatal error config file: %s \n", err))
		}
	} else {
		// println("Loading configuration from gleam.yaml...")
		connections := viper.GetStringMap("connections")
		for k, c := range connections {
			m := c.(map[string]interface{})
			adapterName := m["adapter"].(string)
			// println("Registering connection:", k, "adatper:", adapterName)
			ci := RegisterConnection(k, adapterName)
			for n, v := range m {
				// println("  ", n, "=", v)
				ci.Set(n, v.(string))
			}
		}
	}
}

type ConnectionInfo struct {
	sync.RWMutex
	AdapterName string
	config      map[string]string
}

func RegisterAdapter(name string, fn func() Adapter) {
	AdapterManager.Lock()
	defer AdapterManager.Unlock()
	AdapterManager.adapters[name] = fn
}

func (am *adatperManager) GetAdapter(name string) (Adapter, bool) {
	am.RLock()
	defer am.RUnlock()
	a, ok := am.adapters[name]
	if !ok {
		return nil, ok
	}
	return a(), ok
}

func RegisterConnection(id string, adapterName string) *ConnectionInfo {
	ConnectionManager.Lock()
	defer ConnectionManager.Unlock()
	AdapterManager.RLock()
	defer AdapterManager.RUnlock()

	ci := &ConnectionInfo{
		AdapterName: adapterName,
		config:      make(map[string]string),
	}
	ConnectionManager.connections[id] = ci
	return ci
}

type adatperManager struct {
	sync.RWMutex
	adapters map[string]func() Adapter
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

func (ci *ConnectionInfo) GetAdapter() (Adapter, bool) {
	return AdapterManager.GetAdapter(ci.AdapterName)
}
