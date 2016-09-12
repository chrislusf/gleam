// word_count.go
package main

import (
	"log"
	"os"
	"path/filepath"

	"github.com/chrislusf/gleam/flow"
)

func main() {

	fileNames, err := filepath.Glob("/Users/chris/Downloads/txt/en/ep-08-*.txt")
	if err != nil {
		log.Fatal(err)
	}

	flow.New().Lines(fileNames).Partition(3).ForEach(`
      function(fname)
        -- Open a file for read
        fh,err = io.open(fname)
        if err then return end
		io.stderr:write("reading "..fname.."\n")
        -- line by line
        while true do
          line = fh:read()
          if not line then break end
          writeRow(line)
        end
        -- Following are good form
        fh:close()
      end
    `).FlatMap(`
      function(line)
        if line then
          return line:gmatch("%w+")
        end
      end
    `).LocalSort().Map(`
      function(word)
        return word, 1
      end
    `).Reduce(`
      function(x, y)
        return x + y
      end
    `).Map(`
      function(k, v)
        return k .. " " .. v
      end
    `).Pipe("sort -n -k 2").SaveTextTo(os.Stdout, "%s")

}
