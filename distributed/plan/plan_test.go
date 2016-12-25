package plan

import (
	"os"
	"testing"

	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/pb"
)

func TestPlanning(t *testing.T) {

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

	left.Join(right).Map(`
      function (word, leftCount, rightCount)
	    return word, leftCount, rightCount, leftCount + rightCount
      end
	`).Fprintf(os.Stdout, "%s\t%d + %d = %d\n")

	_, taskGroups := GroupTasks(f)

	for _, taskGroup := range taskGroups {
		println("processing step:", taskGroup.Tasks[0].Step.Name)
		ins := TranslateToInstructionSet(taskGroup)
		PrintInstructionSet(ins)
	}
}

func PrintInstructionSet(instructions *pb.InstructionSet) {
	for _, ins := range instructions.GetInstructions() {
		println(ins.String())
	}
}
