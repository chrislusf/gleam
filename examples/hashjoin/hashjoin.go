package main

import (
	"os"

	"github.com/chrislusf/gleam"
)

func main() {

	join1()
}

func join1() {

	f := gleam.New().Init(`
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
    `).Fprintf(os.Stdout, "hash joined:%s %d\n")

	f.Run()

}
