package distributed

import (
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/chrislusf/gleam/distributed/agent"
	"github.com/chrislusf/gleam/distributed/master"
	"github.com/chrislusf/gleam/flow"
	"github.com/golang/protobuf/proto"
)

func xxTestInstructionSet(t *testing.T) {

	masterPort := ":34563"
	masterAddress := "localhost" + masterPort

	go master.RunMaster(masterPort)

	time.Sleep(time.Second)

	go agent.NewAgentServer(&agent.AgentServerOption{
		Dir:          proto.String("."),
		Host:         proto.String("localhost"),
		Port:         proto.Int(6666),
		Master:       proto.String(masterAddress),
		DataCenter:   proto.String("defaultDataCenter"),
		Rack:         proto.String("defaultRack"),
		MaxExecutor:  proto.Int(8),
		CPULevel:     proto.Int(1),
		MemoryMB:     proto.Int64(1024),
		CleanRestart: proto.Bool(true),
	}).Run()

	time.Sleep(time.Second)

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

	f.Run(Option().SetMaster(masterAddress))

	os.Exit(0)

}
