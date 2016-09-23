// Package master collects data from agents, and manage named network
// channels.
package master

import (
	"crypto/tls"
	"log"
	"net"
	"net/http"
	"sync"

	"github.com/chrislusf/gleam/util"
)

type NamedChannelMap struct {
	name2Chans map[string][]*ChannelInformation
	sync.Mutex
}

func (m *NamedChannelMap) GetChannels(name string) ([]*ChannelInformation, bool) {
	ret, ok := m.name2Chans[name]
	return ret, ok
}

func (m *NamedChannelMap) SetChannels(name string, chans []*ChannelInformation) {
	m.Lock()
	defer m.Unlock()
	m.name2Chans[name] = chans
}

type TeamMaster struct {
	channels       *NamedChannelMap
	MasterResource *MasterResource
}

func (tl *TeamMaster) statusHandler(w http.ResponseWriter, r *http.Request) {
	infos := make(map[string]interface{})
	infos["Version"] = "0.001"
	util.Json(w, r, http.StatusOK, infos)
}

func RunMaster(tlsConfig *tls.Config, listenOn string) {
	tl := &TeamMaster{}
	tl.channels = &NamedChannelMap{name2Chans: make(map[string][]*ChannelInformation)}
	tl.MasterResource = NewMasterResource()

	masterMux := http.NewServeMux()

	masterMux.HandleFunc("/", tl.statusHandler)
	masterMux.HandleFunc("/agent/assign", tl.requestAgentHandler)
	masterMux.HandleFunc("/agent/update", tl.updateAgentHandler)
	masterMux.HandleFunc("/agent/", tl.listAgentsHandler)
	masterMux.HandleFunc("/channel/", tl.handleChannel)

	var listener net.Listener
	var err error
	if tlsConfig == nil {
		listener, err = net.Listen("tcp", listenOn)
	} else {
		listener, err = tls.Listen("tcp", listenOn, tlsConfig)
	}
	if err != nil {
		log.Fatalf("Volume server fail to serve public: %v", err)
	}
	util.SetupHttpClient(tlsConfig)

	if e := http.Serve(listener, masterMux); e != nil {
		log.Fatalf("Volume server fail to serve public: %v", e)
	}

}
