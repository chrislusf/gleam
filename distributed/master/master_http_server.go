// Package master collects data from agents, and manage named network
// channels.
package master

import (
	"net/http"

	"github.com/chrislusf/gleam/distributed/master/ui"
)

var mux map[string]func(http.ResponseWriter, *http.Request)

func init() {
	mux = make(map[string]func(http.ResponseWriter, *http.Request))
	mux["/"] = masterServer.uiStatusHandler
}

func (ms *MasterServer) uiStatusHandler(w http.ResponseWriter, r *http.Request) {
	infos := make(map[string]interface{})
	infos["Version"] = 0.01
	args := struct {
		Version  string
		Topology interface{}
	}{
		"0.01",
		ms.Topology,
	}
	ui.StatusTpl.Execute(w, args)
}
