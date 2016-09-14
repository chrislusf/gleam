// word_count.go
package main

import (
	"flag"
	"log"
	"os"
	"path/filepath"
	"runtime/pprof"

	"github.com/chrislusf/gleam/flow"
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

	fileNames, err := filepath.Glob("/Users/chris/Downloads/txt/en/ep-08-*.txt")
	if err != nil {
		log.Fatal(err)
	}

	flow.New().Strings(fileNames).Partition(3).ForEach(`
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
        return line:gmatch("%w+")
      end
    `).Map(`
      function(word)
        return word, 1
      end
    `).ReduceByKey(`
      function(x, y)
        return x + y
      end
    `).SaveTextTo(os.Stdout, "%s\t%d")

}
