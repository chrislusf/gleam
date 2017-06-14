package main

import (
	"github.com/chrislusf/gleam/distributed"
	"github.com/chrislusf/gleam/flow"
)

func main() {

	f := flow.New().Init(`
	function splitter(line)
        return line:gmatch("%w+")
    end
	`)

	words := f.TextFile(
		"../../flow/dataset_map.go",
	).FlatMap("splitter").RoundRobin(7)

	x := words.Map(`
		function(word)
			return word, 1
		end
	`)
	y := f.Strings([]string{
		"func",
		"return",
	})
	x.HashJoin(y).ReduceBy(`
        function(x,y)
            return x+y
        end
    `).Printlnf("hash joined:%s %d")

	f.Run()

	//f.Run(driver.NewOption().SetMaster(":7777"))

	f.Run(distributed.Planner())

}
