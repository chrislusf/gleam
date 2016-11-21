// Package market is a market system to match tasks with resources.
//
// Continuous Double Auction Protocol
// (CDA) is to allocate the best possible resource to an arriving
// task and to prioritise tasks according to their price
// bid. When a Task Query object arrives at the market the
// protocol searches all available resource offers and returns
// the first occurrence of the ’best’ match, i.e. the
// cheapest or the fastest resource which satisfies the task’s
// constraints. Whenever a resource becomes available and
// there are several tasks waiting, the one with the highest
// price bid is processed first.

// this implmentation only support one supplier

package market

import (
	//"fmt"
	"sync"
)

type Object interface{}

type Requirement interface{}

type Demand struct {
	Requirement Requirement
	Bid         float64
	ReturnChan  chan Supply
}

type Supply struct {
	Object Object
}

type Market struct {
	Demands    []Demand
	Supplies   []Supply
	Lock       sync.Mutex
	ScoreFn    func(Requirement, float64, Object) float64
	FetchFn    func([]Demand)
	hasDemands *sync.Cond
}

func NewMarket() *Market {
	m := &Market{}
	m.hasDemands = sync.NewCond(&m.Lock)
	return m
}

func (m *Market) SetScoreFunction(scorer func(Requirement, float64, Object) float64) *Market {
	m.ScoreFn = scorer
	return m
}

func (m *Market) SetFetchFunction(fn func([]Demand)) *Market {
	m.FetchFn = fn
	return m
}

// retChan should be a buffered channel
func (m *Market) AddDemand(r Requirement, bid float64, retChan chan Supply) {
	m.Lock.Lock()
	defer m.Lock.Unlock()

	if len(m.Supplies) > 0 {
		supply, matched := m.pickBestSupplyFor(r)
		if matched {
			retChan <- supply
			close(retChan)
			return
		}
	}
	m.Demands = append(m.Demands, Demand{
		Requirement: r,
		Bid:         bid,
		ReturnChan:  retChan,
	})
	m.hasDemands.Signal()
}

func (m *Market) FetcherLoop() {
	for {
		// println("FetcherLoop Lock:", len(m.Demands))
		m.Lock.Lock()
		for len(m.Demands) == 0 {
			// println("FetcherLoop wait:", len(m.Demands))
			m.hasDemands.Wait()
		}
		// println("FetcherLoop UnLock:", len(m.Demands))
		m.Lock.Unlock()

		// println("fetching current demands:", len(m.Demands))
		m.FetchFn(m.Demands)
		// println("fetching finished demands:", len(m.Demands))
	}
}

func (m *Market) ReturnSupply(s Supply) {
	m.AddSupply(s)
}

func (m *Market) AddSupply(supply Supply) {
	m.Lock.Lock()
	defer m.Lock.Unlock()

	if len(m.Demands) > 0 {
		demand, matched := m.pickBestDemandFor(supply)
		if matched {
			demand.ReturnChan <- supply
			close(demand.ReturnChan)
			return
		}
	}

	m.Supplies = append(m.Supplies, supply)
}

func (m *Market) pickBestSupplyFor(r Requirement) (ret Supply, matched bool) {

	scores := make([]float64, len(m.Supplies))
	for i, supply := range m.Supplies {
		scores[i] = m.ScoreFn(r, 1, supply.Object)
	}
	maxScore, maxIndex := 0.0, 0
	for i, score := range scores {
		if score > maxScore {
			maxScore = score
			maxIndex = i
			matched = true
		}
	}

	if matched {
		ret = m.Supplies[maxIndex]
		m.Supplies = append(m.Supplies[:maxIndex], m.Supplies[maxIndex+1:]...)
	}

	return ret, matched
}

func (m *Market) pickBestDemandFor(supply Supply) (ret Demand, matched bool) {

	scores := make([]float64, len(m.Demands))
	for i, demand := range m.Demands {
		scores[i] = m.ScoreFn(demand.Requirement, demand.Bid, supply.Object)
	}
	maxScore, maxIndex := 0.0, 0
	for i, score := range scores {
		if score > maxScore {
			maxScore = score
			maxIndex = i
			matched = true
		}
	}

	if matched {
		ret = m.Demands[maxIndex]
		// fmt.Printf("matched demand: %+v\n", ret)
		m.Demands = append(m.Demands[:maxIndex], m.Demands[maxIndex+1:]...)
	}

	return ret, matched
}
