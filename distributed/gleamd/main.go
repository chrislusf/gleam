package main

import (
	"os"

	kingpin "gopkg.in/alecthomas/kingpin.v2"

	exe "github.com/chrislusf/gleam/distributed/executor"
)

var (
	app = kingpin.New("glow", "A command-line net channel.")

	executor               = app.Command("execute", "Execute an instruction set")
	executorInstructionSet = app.Command("execute.instructions", "The instruction set")
)

func main() {

	switch kingpin.MustParse(app.Parse(os.Args[1:])) {

	case executor.FullCommand():
		exe.NewExecutor(nil, nil).Execute()
	}
}
