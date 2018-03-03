package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"runtime/pprof"
	"time"

	"github.com/chrislusf/gleam/distributed"
	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/gio"
	"github.com/chrislusf/gleam/util"
	glow "github.com/chrislusf/glow/flow"
	"sync"
	"sync/atomic"
)

var (
	master = flag.String("master", "localhost:45326", "master server location")

	monteCarloMapperId = gio.RegisterMapper(monteCarloMapper)
	sumReducerId       = gio.RegisterReducer(sumReducer)

	times  = 1024 * 1024 * 2560
	factor = 1024 * 1024
)

func main() {

	gio.Init()
	flag.Parse()

	f, _ := os.Create("p.prof")
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	// uncomment this line if you setup the gleam master and agents
	testPureGoGleam("distributed parallel 7", false)
	testPureGoGleam("local mode parallel 7", true)

	testDirectGo()

	testDirectGoConcurrent()

	// this is not fair since many optimization is not applied
	testLocalFlow()
}

func testPureGoGleam(name string, isLocal bool) {
	var count int64

	startTime := time.Now()
	f := flow.New("pi estimation").
		Source("iteration times", util.Range(0, times/factor)).
		PartitionByKey("partition", 7).
		Map("monte carlo", monteCarloMapperId).
		Reduce("sum", sumReducerId).
		SaveFirstRowTo(&count)

	if isLocal {
		f.Run()
	} else {
		f.Run(distributed.Option().SetMaster(*master).SetProfiling(true))
	}

	fmt.Printf("pi = %f\n", 4.0*float64(count)/float64(times))
	fmt.Printf("pure go gleam %s time cost: %s\n", name, time.Now().Sub(startTime))
	fmt.Println()
}
func testDirectGo() {
	startTime := time.Now()

	r := rand.New(rand.NewSource(time.Now().Unix()))

	var count int
	for i := 0; i < times; i++ {
		x, y := r.Float64(), r.Float64()
		if x*x+y*y < 1 {
			count++
		}
	}
	fmt.Printf("pi = %f\n", 4.0*float64(count)/float64(times))
	fmt.Printf("direct pure go time cost: %s\n", time.Now().Sub(startTime))
	fmt.Println()
}

func testDirectGoConcurrent() {
	startTime := time.Now()

	var wg sync.WaitGroup

	var finalCount int64

	for i := 0; i < 7; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			r := rand.New(rand.NewSource(time.Now().Unix()))

			var count int64
			for i := 0; i < times/7; i++ {
				x, y := r.Float64(), r.Float64()
				if x*x+y*y < 1 {
					count++
				}
			}

			atomic.AddInt64(&finalCount, count)

		}()
	}

	wg.Wait()

	fmt.Printf("pi = %f\n", 4.0*float64(finalCount)/float64(times))
	fmt.Printf("concurrent pure go time cost: %s\n", time.Now().Sub(startTime))
	fmt.Println()
}

func testLocalFlow() {
	startTime := time.Now()
	ch := make(chan int)
	go func() {
		for i := 0; i < times/factor; i++ {
			ch <- i
		}
		close(ch)
	}()
	glow.New().Channel(ch).Partition(7).Map(func(t int) int {
		r := rand.New(rand.NewSource(time.Now().Unix()))

		count := 0
		for i := 0; i < factor; i++ {
			x, y := r.Float32(), r.Float32()
			if x*x+y*y < 1 {
				count++
			}
		}
		return count
	}).Reduce(func(x, y int) int {
		return x + y
	}).Map(func(count int) {
		fmt.Printf("pi = %f\n", 4.0*float64(count)/float64(times))
	}).Run()

	fmt.Printf("flow parallel 7 time cost: %s\n", time.Now().Sub(startTime))
	fmt.Println()
}

func monteCarloMapper(row []interface{}) error {
	r := rand.New(rand.NewSource(time.Now().Unix()))

	var count int
	for i := 0; i < factor; i++ {
		x, y := r.Float32(), r.Float32()
		if x*x+y*y < 1 {
			count++
		}
	}
	gio.Emit(count)
	return nil
}

func sumReducer(x, y interface{}) (interface{}, error) {
	a := x.(int64)
	b := y.(int64)
	return a + b, nil
}
