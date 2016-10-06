package main

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"runtime/pprof"
	"time"

	"github.com/chrislusf/gleam"
	"github.com/chrislusf/gleam/util"
	"github.com/chrislusf/glow/flow"
)

func main() {
	f, _ := os.Create("p.prof")
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	times := 1024 * 1024 * 1024
	networkTrafficReductionFactor := 100000

	benchmark(times, "gleam single pipe", testUnixPipeThroughput)
	benchmark(times, "gleam connected pipe", testGleamUnixPipeThroughput)
	/*
		benchmark(times, "gleam single pipe", testUnixPipeThroughput)
		benchmark(times, "gleam connected pipe", testGleamUnixPipeThroughput)
		testUnixPipe(times)
	*/
	testLocalGleam(times, networkTrafficReductionFactor)
	testDistributedGleam(times, networkTrafficReductionFactor)
	/*
		testLuajit(times)
		testPureGo(times)
		testLocalGleam(times, networkTrafficReductionFactor)
		testLocalFlow(times, networkTrafficReductionFactor)
	*/
}

func benchmark(times int, name string, f func(int)) {
	startTime := time.Now()

	f(times)

	fmt.Printf("%s: %s\n", name, time.Now().Sub(startTime))
	fmt.Println()
}

func testUnixPipeThroughput(times int) {
	out := gleam.New().Strings([]string{"/Users/chris/Downloads/txt/en/ep-08-*.txt"}).PipeAsArgs("cat $1")
	for i := 0; i < 10; i++ {
		out = out.Pipe("cat")
	}
	out.Fprintf(ioutil.Discard, "%s\n")
}

func testGleamUnixPipeThroughput(times int) {
	gleam.New().Strings([]string{"/Users/chris/Downloads/txt/en/ep-08-*.txt"}).PipeAsArgs("cat $1").Pipe("wc").Fprintf(ioutil.Discard, "%s\n")
}

func testUnixPipe(times int) {
	// PipeAsArgs has 4ms cost to startup a process
	startTime := time.Now()
	gleam.New().Source(
		util.Range(0, 100),
	).PipeAsArgs("echo foo bar $1").Fprintf(ioutil.Discard, "%s\n")

	fmt.Printf("gleam pipe time diff: %s\n", time.Now().Sub(startTime))
	fmt.Println()
}

func testDistributedGleam(times int, factor int) {
	var count int64

	startTime := time.Now()
	gleam.NewDistributed().Script("lua", `
      function count(x, y)
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
		return count
      end
    `, factor)).Reduce("count").SaveFirstRowTo(&count)

	fmt.Printf("pi = %f\n", 4.0*float64(count)/float64(times))
	fmt.Printf("gleam distributed time diff: %s\n", time.Now().Sub(startTime))
	fmt.Println()
}

func testLocalGleam(times int, factor int) {
	var count int64
	startTime := time.Now()
	gleam.New().Script("lua", `
      function count(x, y)
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
		return count
      end
    `, factor)).Reduce("count").SaveFirstRowTo(&count)

	fmt.Printf("pi = %f\n", 4.0*float64(count)/float64(times))
	fmt.Printf("gleam local time diff: %s\n", time.Now().Sub(startTime))
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
	flow.New().Channel(ch).Map(func(t int) int {
		count := 0
		for i := 0; i < factor; i++ {
			x, y := rand.Float32(), rand.Float32()
			if x*x+y*y < 1 {
				count++
			}
		}
		return count
	}).LocalReduce(func(x, y int) int {
		return x + y
	}).Map(func(count int) {
		fmt.Printf("pi = %f\n", 4.0*float64(count)/float64(times))
	}).Run()

	fmt.Printf("flow time diff: %s\n", time.Now().Sub(startTime))
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
	gleam.New().Source(util.Range(0, 1)).Map(fmt.Sprintf(`
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
    `, times)).SaveFirstRowTo(&count)

	fmt.Printf("count=%d pi = %f\n", count, 4.0*float64(count)/float64(times))
	fmt.Printf("luajit local time diff: %s\n", time.Now().Sub(startTime))
	fmt.Println()
}
