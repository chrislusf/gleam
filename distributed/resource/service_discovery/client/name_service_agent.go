// Package client send heartbeats to master.
package client

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/chrislusf/gleam/distributed/resource/service_discovery/master"
	"github.com/chrislusf/gleam/util"
)

type NameServiceProxy struct {
	Leaders []string
}

func NewNameServiceProxy(leaders ...string) *NameServiceProxy {
	n := &NameServiceProxy{
		Leaders: leaders,
	}
	return n
}

func (n *NameServiceProxy) Find(name string) (locations []string) {
	for _, l := range n.Leaders {
		jsonBlob, err := util.Get(util.SchemePrefix + l + "/channel/" + name)
		if err != nil {
			log.Printf("Failed to list from %s:%v", l, err)
		}
		var ret []master.ChannelInformation
		err = json.Unmarshal(jsonBlob, &ret)
		if err != nil {
			fmt.Printf("%s/list%s unmarshal error:%v, json:%s", l, name, err, string(jsonBlob))
			continue
		}
		if len(ret) <= 0 {
			return nil
		}
		for _, rs := range ret {
			locations = append(locations, rs.Location)
		}
	}
	return locations
}
