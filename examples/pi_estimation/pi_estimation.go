package main

import (
	"fmt"
	"time"

	"github.com/chrislusf/gleam"
	"github.com/chrislusf/gleam/util"
)

func main() {

	var count int64
	times := 1024 * 1024 * 1

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
	fmt.Printf("time diff: %s\n", time.Now().Sub(startTime))

	startTime = time.Now()
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
	fmt.Printf("time diff: %s\n", time.Now().Sub(startTime))

}
