package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"time"

	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/gio"
	"github.com/chrislusf/gleam/gio/mapper"
	"github.com/chrislusf/gleam/gio/reducer"
	"github.com/chrislusf/gleam/plugins/file"
	"github.com/chrislusf/gleam/util"
)

var (
	isCron = flag.Bool("cron", false, "run as cron or not")
	driver = flag.Int("driver", 1, "the current running driver")
)

type Handler func() *flow.Dataset

const (
	_ = iota
	inGo
	inPipe
	modeNum
)

func makeWCInGo() *flow.Dataset {
	return flow.New("top5 words in passwd").
		Read(file.Txt("/etc/passwd", 1)).
		Map("tokenize", mapper.Tokenize).     // invoke the registered "tokenize" mapper function.
		Map("addOne", mapper.AppendOne).      // invoke the registered "addOne" mapper function.
		ReduceByKey("sum", reducer.SumInt64). // invoke the registered "sum" reducer function.
		Sort("sortBySum", flow.OrderBy(2, true)).
		Top("top5", 5, flow.OrderBy(2, false)).
		Printlnf("%s\t%d")
}

func makeWCInPipeline() *flow.Dataset {
	return flow.New("word count by unix pipes").
		Read(file.Txt("/etc/passwd", 2)).
		Map("tokenize", mapper.Tokenize).
		Pipe("lowercase", "tr 'A-Z' 'a-z'").
		Pipe("write", "tee x.out").
		Pipe("sort", "sort").
		Pipe("uniq", "uniq -c").
		OutputRow(func(row *util.Row) error {
			fmt.Printf("%s\n", gio.ToString(row.K[0]))
			return nil
		})
}

var handlers = map[int]Handler{
	inGo:   makeWCInGo,
	inPipe: makeWCInPipeline,
}

func run(mode int, at string) {
	exe, _ := os.Executable()
	cmd := exec.Command(exe, fmt.Sprintf("-driver=%d", mode))
	cmd.Stdout = os.Stdout
	err := cmd.Run()
	fmt.Printf("run at %s with mode: %d and error: %v\n", at, mode, err)
}

func doCron() {
	t := time.NewTicker(5e9)
	m := inGo
FOR_LOOP:
	for {
		select {
		case now := <-t.C:
			if m == modeNum {
				break FOR_LOOP
			}
			run(m, now.String())
			m++
		}

	}

}
func main() {
	flag.Parse() // optional, since gio.Init() will call this also.

	if *isCron {
		doCron()
		return
	}

	gio.Init() // If the command line invokes the mapper or reducer, execute it and exit.

	if h, ok := handlers[*driver]; ok {
		h().Run()
	}

}
