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

	times := 1024 * 1024 * 10

	testUnixPipe(times)
	//testLuajit(times)
	//testPureGo(times)
	//testLocalGleam(times)
	//testLocalFlow(times)
	//testDistributedGleam(times)
}

func testUnixPipe(times int) {
	startTime := time.Now()
	gleam.New().Source(
		util.Range(1, 100, 1),
	).PipeAsArgs("echo foo bar $1").Fprintf(ioutil.Discard, "%s\n")

	fmt.Printf("gleam pipe time diff: %s\n", time.Now().Sub(startTime))
	fmt.Println()
}

func testDistributedGleam(times int) {
	var count int64

	startTime := time.Now()
	gleam.NewDistributed().Script("lua", `
      function count(x, y)
        return x + y
      end
    `).Source(util.Range(1, times, 1)).Partition(1).Map(`
      function(n)
        local x, y = math.random(), math.random()
        if x*x+y*y < 1 then
          return 1
        end
      end
    `).Reduce("count").SaveOneRowTo(&count)

	fmt.Printf("pi = %f\n", 4.0*float64(count)/float64(times))
	fmt.Printf("gleam distributed time diff: %s\n", time.Now().Sub(startTime))
	fmt.Println()
}

func testLocalGleam(times int) {
	var count int64
	startTime := time.Now()
	gleam.New().Script("lua", `
      function count(x, y)
        return x + y
      end
    `).Source(util.Range(1, times, 1)).Partition(2).Map(`
      function(n)
        local x, y = math.random(), math.random()
        if x*x+y*y < 1 then
          return 1
        end
      end
    `).Reduce("count").SaveOneRowTo(&count)

	fmt.Printf("pi = %f\n", 4.0*float64(count)/float64(times))
	fmt.Printf("gleam local time diff: %s\n", time.Now().Sub(startTime))
	fmt.Println()
}

func testLocalFlow(times int) {
	startTime := time.Now()
	ch := make(chan int)
	go func() {
		for i := 0; i < times; i++ {
			ch <- i
		}
		close(ch)
	}()
	flow.New().Channel(ch).Map(func(t int) int {
		x, y := rand.Float32(), rand.Float32()
		if x*x+y*y < 1 {
			return 1
		}
		return 0
	}).LocalReduce(func(x, y int) int {
		return x + y
	}).Map(func(count int) {
		println("=>", count)
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
	gleam.New().Source(util.Range(1, 1, 1)).Map(fmt.Sprintf(`
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
    `, times)).SaveOneRowTo(&count)

	fmt.Printf("pi = %f\n", 4.0*float64(count)/float64(times))
	fmt.Printf("luajit local time diff: %s\n", time.Now().Sub(startTime))
	fmt.Println()
}
