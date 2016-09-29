package driver

import (
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/chrislusf/gleam/distributed/cmd"
	"github.com/chrislusf/gleam/flow"
)

func TestInstructionSet(t *testing.T) {

	fileNames, err := filepath.Glob("/Users/chris/Downloads/txt/en/ep-08-03-*.txt")
	if err != nil {
		log.Fatal(err)
	}

	f := flow.New().SetRunner(Distributed)
	f.Strings(fileNames).Partition(3).PipeAsArgs("ls -l $1").FlatMap(`
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
    `).Map(`
      function(k, v)
        return k .. " " .. v
      end
    `).Pipe("sort -n -k 2").Fprintf(os.Stdout, "%s\n")

}

func TestPlanning(t *testing.T) {

	f := flow.New().SetRunner(Distributed).Script("lua", `
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

	left.Join(right).Map(`
      function (word, leftCount, rightCount)
	    return word, leftCount, rightCount, leftCount + rightCount
      end
	`).Fprintf(os.Stdout, "%s\t%d + %d = %d\n")

}
