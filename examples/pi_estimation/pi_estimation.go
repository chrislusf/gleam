package main

import (
	"fmt"

	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/util"
)

func main() {

	var count int64
	times := 1024 * 1024 * 1

	flow.New().Script("lua", `
      function count(x, y)
        return x + y
      end
    `).Source(util.Range(1, times, 1)).Partition(8).Map(`
      function(n)
        local x, y = math.random(), math.random()
        if x*x+y*y < 1 then
          return 1
        end
      end
    `).Reduce("count").SaveOneRowTo(&count)

	fmt.Printf("pi = %f\n", 4.0*float64(count)/float64(times))

}
