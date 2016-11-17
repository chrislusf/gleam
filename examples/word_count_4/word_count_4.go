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

	//fileNames, err := filepath.Glob("/Users/chris/Downloads/txt/en/ep-08-*.txt")
	fileNames, err := filepath.Glob("/etc/passwd")
	if err != nil {
		log.Fatal(err)
	}

	flow.New().Strings(fileNames).Partition(1).ForEach(`
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
    `).Pipe("tr 'A-Z' 'a-z'").Map(`
      function(word)
        return word, 1
      end
    `).ReduceBy(`
      function(x, y)
        return x + y
      end
    `).Top(5, flow.OrderBy(2, true)).Fprintf(os.Stdout, "%s\t%d\n").Run()

}
