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
)

var (
	master = flag.String("master", "localhost:45326", "master server location")

	monteCarloMapperId = gio.RegisterMapper(monteCarloMapper)
	sumReducerId       = gio.RegisterReducer(sumReducer)

	times  = 1024 * 1024 * 80
	factor = 1024 * 1024
)

func main() {
	flag.Parse()
	gio.Init()

	f, _ := os.Create("p.prof")
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	// uncomment this line if you setup the gleam master and agents
	testPureGoGleam("distributed parallel 7", false)
	testPureGoGleam("local mode parallel 7", true)

	testGleam("distributed parallel 7", false)
	testGleam("local mode parallel 7", true)

	testDirectGo()
	testLuajit()

	// this is not fair since many optimization is not applied
	testLocalFlow()
}

func testGleam(name string, isLocal bool) {
	var count int64
	startTime := time.Now()
	f := flow.New().Init(`
      function sum(x, y)
        return x + y
      end
    `).Source(util.Range(0, times/factor)).Partition(7).Map(fmt.Sprintf(`
      function(n)
	    local count = 0
	    for i=1,%d,1 do
          local x, y = math.random(), math.random()
          if x*x+y*y < 1 then
            count = count + 1
          end
		end
		-- log("count = "..count)
		return count
      end
    `, factor)).Reduce("sum").SaveFirstRowTo(&count)

	if isLocal {
		f.Run()
	} else {
		f.Run(distributed.Option().SetMaster(*master))
	}

	fmt.Printf("pi = %f\n", 4.0*float64(count)/float64(times))
	fmt.Printf("gleam %s time cost: %s\n", name, time.Now().Sub(startTime))
	fmt.Println()
}

func testPureGoGleam(name string, isLocal bool) {
	var count int64
	startTime := time.Now()
	f := flow.New().Init(`
      function sum(x, y)
        return x + y
      end
    `).Source(util.Range(0, times/factor)).Partition(7).
		Mapper(monteCarloMapperId).
		Reduce("sum").SaveFirstRowTo(&count)

	if isLocal {
		f.Run()
	} else {
		f.Run(distributed.Option().SetMaster(*master))
	}

	fmt.Printf("pi = %f\n", 4.0*float64(count)/float64(times))
	fmt.Printf("pure go gleam %s time cost: %s\n", name, time.Now().Sub(startTime))
	fmt.Println()
}
func testDirectGo() {
	startTime := time.Now()

	var count int
	for i := 0; i < times; i++ {
		x, y := rand.Float64(), rand.Float64()
		if x*x+y*y < 1 {
			count++
		}
	}
	fmt.Printf("pi = %f\n", 4.0*float64(count)/float64(times))
	fmt.Printf("direct pure go time cost: %s\n", time.Now().Sub(startTime))
	fmt.Println()
}

func testLuajit() {
	var count int64
	startTime := time.Now()
	flow.New().Source(util.Range(0, 1)).Map(fmt.Sprintf(`
      function(n)
	    local count = 0
	    for i=1,%d,1 do
          local x, y = math.random(), math.random()
          if x*x+y*y < 1 then
            count = count + 1
          end
		end
		return count
      end
    `, times)).SaveFirstRowTo(&count).Run()

	fmt.Printf("count=%d pi = %f\n", count, 4.0*float64(count)/float64(times))
	fmt.Printf("luajit local time cost: %s\n", time.Now().Sub(startTime))
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
		count := 0
		for i := 0; i < factor; i++ {
			x, y := rand.Float32(), rand.Float32()
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
	var count int
	for i := 0; i < factor; i++ {
		x, y := rand.Float32(), rand.Float32()
		if x*x+y*y < 1 {
			count++
		}
	}
	gio.Emit(count)
	return nil
}

func sumReducer(x, y interface{}) (interface{}, error) {
	a := x.(uint64)
	b := y.(uint64)
	return a + b, nil
}
