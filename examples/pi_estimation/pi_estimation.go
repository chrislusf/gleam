package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime/pprof"

	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/util"
	"github.com/chrislusf/gleam/util/on_interrupt"
)

var (
	cpuProfile = flag.String("cpuprofile", "", "cpu profile output file")
)

func main() {
	flag.Parse()

	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
		on_interrupt.OnInterrupt(func() {
			pprof.StopCPUProfile()
		}, func() {
			pprof.StopCPUProfile()
		})
	}

	var count int64
	times := 1024 * 1024 * 1

	flow.New().Script("lua", `
      function count(x, y)
        return x + y
      end
	`).Source(util.Range(1, times, 1)).Partition(8).Map(`
	  function(n)
	    local x, y = math.random(), math.random()
		-- log(n .. " x="..x.." y="..y)
		if x*x+y*y < 1 then
		  return 1
		end
	  end
	`).Reduce("count").SaveFinalRowTo(&count)

	fmt.Printf("pi = %f\n", 4.0*float64(count)/float64(times))

}
