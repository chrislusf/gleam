package main

import (
	"os"

	"github.com/chrislusf/gleam/flow"
)

func main() {

	f := flow.New()
	left := f.TextFile("../../flow/dataset_map.go").FlatMap(`
        function(line)
            if line then
                return line:gmatch("%w+")
            end
        end
    `).Pipe("sort").Pipe("uniq -c").Map(`
        function(line)
			line = line:gsub("^%s*", "")
            index = string.find(line, " ")
            return line:sub(index+1), tonumber(line:sub(1,index-1))
        end
    `)
	right := f.TextFile("../../flow/dataset_output.go").FlatMap(`
        function(line)
            if line then
                return line:gmatch("%w+")
            end
        end
    `).Pipe("sort").Pipe("uniq -c").Map(`
        function(line)
			line = line:gsub("^%s*", "")
            index = string.find(line, " ")
            return line:sub(index+1), tonumber(line:sub(1,index-1))
        end
    `)

	left.Join(right).SaveTextTo(os.Stdout, "%s,%d,%d")

}
