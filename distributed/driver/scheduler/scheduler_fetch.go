package scheduler

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/url"
	"time"

	"github.com/chrislusf/gleam/distributed/driver/scheduler/market"
	"github.com/chrislusf/gleam/distributed/plan"
	"github.com/chrislusf/gleam/distributed/resource"
	"github.com/chrislusf/gleam/util"
)

// Requirement is TaskGroup
// Object is Agent's Location
func (s *Scheduler) Fetch(demands []market.Demand) {
	var request resource.AllocationRequest
	for _, d := range demands {
		demand := d.Requirement.(*plan.TaskGroup)
		request.Requests = append(request.Requests, resource.ComputeRequest{
			ComputeResource: resource.ComputeResource{
				CPUCount: 1,
				CPULevel: 1,
				MemoryMB: int64(s.Option.TaskMemoryMB),
			},
			Inputs: s.findTaskGroupInputs(demand),
		})
	}

	result, err := Assign(s.Leader, &request)
	if err != nil {
		log.Printf("%s Failed to allocate: %v", s.Leader, err)
		time.Sleep(time.Millisecond * time.Duration(15000+rand.Int63n(5000)))
	} else {
		if len(result.Allocations) == 0 {
			log.Printf("%s Failed to allocate any executor.", s.Leader)
			time.Sleep(time.Millisecond * time.Duration(2000+rand.Int63n(1000)))
		} else {
			log.Printf("%s allocated %d executors.", s.Leader, len(result.Allocations))
			for _, allocation := range result.Allocations {
				s.Market.AddSupply(market.Supply{
					Object: allocation,
				})
			}
		}
	}
}

func (s *Scheduler) findTaskGroupInputs(tg *plan.TaskGroup) (ret []resource.DataResource) {
	firstTask := tg.Tasks[0]
	for _, input := range firstTask.InputShards {
		_, dataLocation, found := s.GetShardLocation(input)
		if !found {
			// log.Printf("Strange2: %s not allocated yet.", input.Name())
			continue
		}
		ret = append(ret, resource.DataResource{
			Location:   dataLocation,
			DataSizeMB: 1, // TODO: read previous run's size
		})
	}
	return
}

func Assign(leader string, request *resource.AllocationRequest) (*resource.AllocationResult, error) {
	values := make(url.Values)
	requestBlob, _ := json.Marshal(request)
	values.Add("request", string(requestBlob))
	jsonBlob, err := util.Post(util.SchemePrefix+leader+"/agent/assign", values)
	if err != nil {
		return nil, err
	}
	var ret resource.AllocationResult
	err = json.Unmarshal(jsonBlob, &ret)
	if err != nil {
		return nil, fmt.Errorf("/agent/assign result JSON unmarshal error:%v, json:%s", err, string(jsonBlob))
	}
	if ret.Error != "" {
		return nil, fmt.Errorf("/agent/assign error:%v", ret.Error)
	}
	return &ret, nil
}
