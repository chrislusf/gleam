package driver

import (
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/chrislusf/gleam/flow"
)

func TestInstructionSet(t *testing.T) {

	fileNames, err := filepath.Glob("../../flow/*.go")
	if err != nil {
		log.Fatal(err)
	}

	f := flow.New()
	f.Strings(fileNames).Partition(3).PipeAsArgs("ls -l $1").FlatMap(`
      function(line)
        return line:gmatch("%w+")
      end
    `).Map(`
      function(word)
        return word, 1
      end
    `).ReduceBy(`
      function(x, y)
        return x + y
      end
    `).Map(`
      function(k, v)
        return k .. " " .. v
      end
    `).Pipe("sort -n -k 2").Fprintf(os.Stdout, "%s\n")

}
