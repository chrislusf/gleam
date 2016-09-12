// word_count.go
package main

import (
	"log"
	"os"
	"path/filepath"

	"github.com/chrislusf/gleam/flow"
)

func main() {

	fileNames, err := filepath.Glob("/Users/chris/Downloads/txt/en/ep-08-03-*.txt")
	if err != nil {
		log.Fatal(err)
	}

	flow.New().Lines(fileNames).Partition(1).ForEach(`
      function(fname)
        -- Open a file for read
        local fh,err = io.open(fname)
        if err then return end
		-- io.stderr:write("reading "..fname.."\n")
        -- line by line
        while true do
          local line = fh:read()
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
    `).Map(`
      function(word)
        return word, 1
      end
    `).Reduce(`
		function(x, y)
			return x + y
		end
	`).LocalSort().SaveTextTo(os.Stdout, "%s\t%d")

}
