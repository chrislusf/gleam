package master

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/chrislusf/gleam/distributed/resource"
	"github.com/chrislusf/gleam/util"
)

func (tl *TeamMaster) requestAgentHandler(w http.ResponseWriter, r *http.Request) {
	requestBlob := []byte(r.FormValue("request"))
	var request resource.AllocationRequest
	err := json.Unmarshal(requestBlob, &request)
	if err != nil {
		util.Error(w, r, http.StatusBadRequest, fmt.Sprintf("request JSON unmarshal error:%v, json:%s", err, string(requestBlob)))
		return
	}

	// fmt.Printf("request:\n%+v\n", request)

	result := tl.allocate(&request)
	// fmt.Printf("result: %v\n%+v\n", result.Error, result.Allocations)
	if result.Error != "" {
		util.Json(w, r, http.StatusNotFound, result)
		return
	}

	util.Json(w, r, http.StatusAccepted, result)

}

func (tl *TeamMaster) updateAgentHandler(w http.ResponseWriter, r *http.Request) {
	servicePortString := r.FormValue("servicePort")
	servicePort, err := strconv.Atoi(servicePortString)
	if err != nil {
		log.Printf("Strange: servicePort not found: %s, %v", servicePortString, err)
	}
	// print("agent address:", r.RemoteAddr)
	host := r.FormValue("serviceIp")
	if host == "" {
		host = r.RemoteAddr
		if strings.Contains(host, ":") {
			host = host[0:strings.LastIndex(host, ":")]
		}
	}
	// println("received agent update from", host+":"+servicePort)
	res, alloc := resource.NewComputeResourceFromRequest(r)
	ai := &resource.AgentInformation{
		Location: resource.Location{
			DataCenter: r.FormValue("dataCenter"),
			Rack:       r.FormValue("rack"),
			Server:     host,
			Port:       servicePort,
		},
		Resource:  res,
		Allocated: alloc,
	}

	// fmt.Printf("reported allocated: %v\n", alloc)

	tl.MasterResource.UpdateAgentInformation(ai)

	w.WriteHeader(http.StatusAccepted)
}
