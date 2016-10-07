package main

import (
	"os"

	"github.com/chrislusf/gleam"
)

func main() {

	f := gleam.New().Script("lua", `
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
	`).Fprintf(os.Stdout, "%s\t%d + %d + %d = %d\n")

}
