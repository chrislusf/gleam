// Package master collects data from agents, and manage named network
// channels.
package master

import (
	"crypto/tls"
	"log"
	"net"
	"net/http"
)

type TeamMaster struct {
	MasterResource *MasterResource
}

func RunMaster(tlsConfig *tls.Config, listenOn string) {
	tl := &TeamMaster{}
	tl.MasterResource = NewMasterResource()

	masterMux := http.NewServeMux()

	masterMux.HandleFunc("/agent/assign", tl.requestAgentHandler)
	masterMux.HandleFunc("/agent/update", tl.updateAgentHandler)

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

	if e := http.Serve(listener, masterMux); e != nil {
		log.Fatalf("Volume server fail to serve public: %v", e)
	}

}
