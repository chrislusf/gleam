package market

import (
	"math"
	"testing"
)

func TestMarket(t *testing.T) {

	m := NewMarket().SetScoreFunction(func(r Requirement, bid float64, obj Object) float64 {
		source := r.(float64)
		target := obj.(float64)
		return 1 / (1 + math.Abs(source-target))
	}).SetFetchFunction(func([]Demand) {
	})

	chs := make([]chan Supply, 3)
	for i := 0; i < 3; i++ {
		chs[i] = make(chan Supply, 1)
	}

	m.AddDemand(Requirement(1.0), 1, chs[0])
	m.AddDemand(Requirement(2.0), 1, chs[1])
	m.AddDemand(Requirement(3.0), 1, chs[2])

	m.AddSupply(Supply{Object(2.8)})
	m.AddSupply(Supply{Object(0.8)})
	m.AddSupply(Supply{Object(1.8)})

	for i := 0; i < 3; i++ {
		s := <-chs[i]
		t.Logf("Got: %f", s.Object.(float64))
	}

	t.Logf("market works well")

}
