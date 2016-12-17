package client

import (
	"fmt"
	"math/rand"
	"net/url"
	"strconv"
	"time"

	"github.com/chrislusf/gleam/util"
)

type HeartBeater struct {
	Leaders      []string
	ServiceIp    string
	ServicePort  int
	SleepSeconds int64
}

func NewHeartBeater(ip string, localPort int, leader string) *HeartBeater {
	h := &HeartBeater{
		Leaders:      []string{leader},
		ServiceIp:    ip,
		ServicePort:  localPort,
		SleepSeconds: 10,
	}
	return h
}

// Starts heart beating
func (h *HeartBeater) StartAgentHeartBeat(killChan chan bool, fn func(url.Values)) {
	connected := false
	for {
		ret := h.beat(func(values url.Values) string {
			fn(values)
			return "/agent/update"
		})
		if ret == true && connected == false {
			fmt.Printf("connected with master %s\n", h.Leaders)
		}
		connected = ret
		select {
		case <-killChan:
			return
		default:
			time.Sleep(time.Duration(rand.Int63n(h.SleepSeconds/2)+h.SleepSeconds/2) * time.Second)
		}
	}
}

func (h *HeartBeater) beat(fn func(url.Values) string) bool {
	values := make(url.Values)
	beatToPath := fn(values)
	values.Add("servicePort", strconv.Itoa(h.ServicePort))
	values.Add("serviceIp", h.ServiceIp)
	ret := false
	for _, leader := range h.Leaders {
		// println("heart beat to", leader, beatToPath)
		_, err := util.Post(util.SchemePrefix+leader+beatToPath, values)
		if err != nil {
			println("Failed to heart beat to", leader, beatToPath)
		} else {
			ret = true
		}
	}
	return ret
}
