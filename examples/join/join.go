package main

import (
	"os"

	"github.com/chrislusf/gleam/flow"
)

func main() {

	join1()
	join2()

}

func join1() {

	f := flow.New().Init(`
	function splitter(line)
        return line:gmatch("%w+")
    end
	`)

	words := f.TextFile(
		"../../flow/dataset_map.go",
	).FlatMap("splitter")

	x := words.Map(`
		function(word)
            --log("word x:"..word)
			return word, 1
		end
	`).ReduceBy(`function(x,y) return x+y end`)
	y := words.Map(`
		function(word)
            --log("word y:"..word)
			return word, 2
		end
	`).ReduceBy(`function(x,y) return x+y end`)

	x.Join(y).Fprintf(os.Stdout, "join1:%s %d + %d\n")

	f.Run()

}

func join2() {

	f := flow.New().Init(`
	function splitter(line)
        return line:gmatch("%w+")
    end
    function parseUniqDashC(line)
      line = line:gsub("^%s*", "")
      index = string.find(line, " ")
      return line:sub(index+1), tonumber(line:sub(1,index-1))
    end
	`)

	left := f.TextFile(
		"../../flow/dataset_map.go",
	).FlatMap("splitter").Pipe("sort").Pipe("uniq -c").Map("parseUniqDashC")

	right := f.TextFile(
		"../../flow/dataset_output.go",
	).FlatMap("splitter").Pipe("sort").Pipe("uniq -c").Map("parseUniqDashC")

	// test self join, and common join
	left.Join(left).Join(right).Map(`
      function (word, leftCount1, leftCount2, rightCount)
	    return word, leftCount1, leftCount2, rightCount, leftCount1 + leftCount2 + rightCount
      end
	`).Fprintf(os.Stdout, "join2:%s\t%d + %d + %d = %d\n").Run()

}
