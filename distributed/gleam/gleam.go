package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"

	kingpin "gopkg.in/alecthomas/kingpin.v2"

	a "github.com/chrislusf/gleam/distributed/agent"
	exe "github.com/chrislusf/gleam/distributed/executor"
	m "github.com/chrislusf/gleam/distributed/master"
	"github.com/chrislusf/gleam/distributed/netchan"
	"github.com/chrislusf/gleam/pb"
	"github.com/chrislusf/gleam/util"
	"github.com/chrislusf/gleam/util/on_interrupt"
	"github.com/golang/protobuf/proto"
)

var (
	app = kingpin.New("gleam", "distributed gleam, acts as master, agent, or executor")

	master        = app.Command("master", "Start a master process")
	masterAddress = master.Flag("address", "listening address host:port").Default(":45326").String()
	masterLogDir  = master.Flag("logDirectory", "a directory to store execution logs").Default(os.TempDir()).String()

	executor     = app.Command("execute", "Execute an instruction set")
	executorNote = executor.Flag("note", "description").String()

	agent       = app.Command("agent", "Agent that can accept read, write requests, manage executors")
	agentOption = &a.AgentServerOption{
		Dir:          agent.Flag("dir", "agent folder to store computed data").Default(os.TempDir()).String(),
		Host:         agent.Flag("host", "agent listening host address. Required in 2-way SSL mode.").Default("localhost").String(),
		Port:         agent.Flag("port", "agent listening port").Default("45327").Int32(),
		Master:       agent.Flag("master", "master address").Default("localhost:45326").String(),
		DataCenter:   agent.Flag("dataCenter", "data center name").Default("defaultDataCenter").String(),
		Rack:         agent.Flag("rack", "rack name").Default("defaultRack").String(),
		MaxExecutor:  agent.Flag("executor.max", "upper limit of executors").Default(strconv.Itoa(runtime.NumCPU())).Int32(),
		CPULevel:     agent.Flag("executor.cpu.level", "relative computing power of single cpu core").Default("1").Int32(),
		MemoryMB:     agent.Flag("memory", "memory limit in MB").Default("1024").Int64(),
		CleanRestart: agent.Flag("clean.restart", "clean up previous dataset files").Default("true").Bool(),
	}
	cpuProfile = agent.Flag("cpuprofile", "cpu profile output file").Default("").String()

	writer             = app.Command("write", "Write data to a topic, input from console")
	writeTopic         = writer.Flag("topic", "Name of a topic").Required().String()
	writerAgentAddress = writer.Flag("agent", "agent host:port").Default("localhost:45327").String()
	writeToDisk        = writer.Flag("onDisk", "write to memory").Default("false").Bool()

	reader             = app.Command("read", "Read data from a topic, output to console")
	readTopic          = reader.Flag("topic", "Name of a source topic").Required().String()
	readerAgentAddress = reader.Flag("agent", "agent host:port").Default("localhost:45327").String()
	readFromDisk       = reader.Flag("onDisk", "read from memory").Default("false").Bool()
)

func main() {

	switch kingpin.MustParse(app.Parse(os.Args[1:])) {

	case master.FullCommand():
		println("master listening on", *masterAddress)
		m.RunMaster(*masterAddress, *masterLogDir)

	case executor.FullCommand():

		rawData, err := ioutil.ReadAll(os.Stdin)
		if err != nil {
			log.Fatalf("failed to read stdin: %v", err)
		}
		instructionSet := pb.InstructionSet{}
		if err := proto.Unmarshal(rawData, &instructionSet); err != nil {
			log.Fatal("unmarshaling instructions error: ", err)
		}

		if instructionSet.IsProfiling {
			// profiling the gleam executor
			profilingFile := fmt.Sprintf("exe%d-%s.pprof", instructionSet.GetFlowHashCode(), strings.Join(instructionSet.InstructionNames(), "-"))
			pwd, _ := os.Getwd()
			println("saving exe pprof to", pwd+"/"+profilingFile)
			f, err := os.Create(profilingFile)
			if err != nil {
				log.Fatal(err)
			}
			pprof.StartCPUProfile(f)
			defer pprof.StopCPUProfile()
		}

		if err := exe.NewExecutor(&exe.ExecutorOption{
			AgentAddress: instructionSet.AgentAddress,
		}, &instructionSet).ExecuteInstructionSet(); err != nil {
			log.Fatalf("Failed task %s: %v", *executorNote, err)
		}

	case writer.FullCommand():

		inChan := util.NewPiper()
		var wg sync.WaitGroup
		wg.Add(1)
		go netchan.DialWriteChannel(context.Background(), &wg, "stdin", *writerAgentAddress, *writeTopic, *writeToDisk, inChan.Reader, 1)
		wg.Add(1)
		go util.LineReaderToChannel(&wg, &pb.InstructionStat{}, "stdin", os.Stdin, inChan.Writer, true, os.Stderr)
		wg.Wait()

	case reader.FullCommand():

		outChan := util.NewPiper()
		var wg sync.WaitGroup
		wg.Add(1)
		go netchan.DialReadChannel(context.Background(), &wg, "stdout", *readerAgentAddress, *readTopic, *readFromDisk, outChan.Writer)
		wg.Add(1)
		util.ChannelToLineWriter(&wg, &pb.InstructionStat{}, "stdout", outChan.Reader, os.Stdout, os.Stderr)
		wg.Wait()

	case agent.FullCommand():

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

		a.RunAgentServer(agentOption)
	}
}
