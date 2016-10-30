package main

import (
	"fmt"
	"math/rand"
	"os"
	"runtime/pprof"
	"time"

	"github.com/chrislusf/gleam/distributed"
	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/util"
	glow "github.com/chrislusf/glow/flow"
)

func main() {
	f, _ := os.Create("p.prof")
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	times := 1024 * 1024 * 16
	networkTrafficReductionFactor := 1024 * 1024

	// uncomment this line if you setup the gleam master and agents
	testGleam("distributed parallel 7", false, times, networkTrafficReductionFactor)
	testGleam("local mode parallel 7", true, times, networkTrafficReductionFactor)

	testPureGo(times)
	testLuajit(times)

	// this is not fair since many optimization is not applied
	testLocalFlow(times, networkTrafficReductionFactor)
}

func testGleam(name string, isLocal bool, times int, factor int) {
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
		f.Run(distributed.Option())
	}

	fmt.Printf("pi = %f\n", 4.0*float64(count)/float64(times))
	fmt.Printf("gleam %s time diff: %s\n", name, time.Now().Sub(startTime))
	fmt.Println()
}

func testPureGo(times int) {
	startTime := time.Now()

	var count int
	for i := 0; i < times; i++ {
		x, y := rand.Float64(), rand.Float64()
		if x*x+y*y < 1 {
			count++
		}
	}
	fmt.Printf("pi = %f\n", 4.0*float64(count)/float64(times))
	fmt.Printf("pure go time diff: %s\n", time.Now().Sub(startTime))
	fmt.Println()
}

func testLuajit(times int) {
	startTime := time.Now()
	var count int64
	startTime = time.Now()
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
	fmt.Printf("luajit local time diff: %s\n", time.Now().Sub(startTime))
	fmt.Println()
}

func testLocalFlow(times int, factor int) {
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

	fmt.Printf("flow time diff: %s\n", time.Now().Sub(startTime))
	fmt.Println()
}
