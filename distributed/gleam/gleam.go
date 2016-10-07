package main

import (
	"io/ioutil"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"sync"

	kingpin "gopkg.in/alecthomas/kingpin.v2"

	a "github.com/chrislusf/gleam/distributed/agent"
	"github.com/chrislusf/gleam/distributed/cmd"
	exe "github.com/chrislusf/gleam/distributed/executor"
	"github.com/chrislusf/gleam/distributed/netchan"
	m "github.com/chrislusf/gleam/distributed/resource/service_discovery/master"
	"github.com/chrislusf/gleam/util"
	"github.com/chrislusf/gleam/util/on_interrupt"
	"github.com/golang/protobuf/proto"
)

var (
	app = kingpin.New("gleamd", "distributed gleam, acts as master, agent, or executor")

	master        = app.Command("master", "Start a master process")
	masterAddress = master.Flag("address", "listening address host:port").Default(":45326").String()

	executor               = app.Command("execute", "Execute an instruction set")
	executorInstructionSet = executor.Flag("steps", "The instruction set").String()

	agent       = app.Command("agent", "Agent that can accept read, write requests, manage executors")
	agentOption = &a.AgentServerOption{
		Dir:          agent.Flag("dir", "agent folder to store computed data").Default(os.TempDir()).String(),
		Host:         agent.Flag("host", "agent listening host address. Required in 2-way SSL mode.").Default("").String(),
		Port:         agent.Flag("port", "agent listening port").Default("45327").Int(),
		Master:       agent.Flag("master", "master address").Default("localhost:45326").String(),
		DataCenter:   agent.Flag("dataCenter", "data center name").Default("defaultDataCenter").String(),
		Rack:         agent.Flag("rack", "rack name").Default("defaultRack").String(),
		MaxExecutor:  agent.Flag("executor.max", "upper limit of executors").Default(strconv.Itoa(runtime.NumCPU())).Int(),
		CPULevel:     agent.Flag("executor.cpu.level", "relative computing power of single cpu core").Default("1").Int(),
		MemoryMB:     agent.Flag("executor.memory", "memory size in MB").Default("1024").Int64(),
		CleanRestart: agent.Flag("clean.restart", "clean up previous dataset files").Default("true").Bool(),
		InMemory:     agent.Flag("memory.only", "data do not spill to disk but back pressured").Bool(),
	}
	cpuProfile = agent.Flag("cpuprofile", "cpu profile output file").Default("").String()

	writer             = app.Command("write", "Write data to a topic")
	writeTopic         = writer.Flag("topic", "Name of a topic").Required().String()
	writerAgentAddress = writer.Flag("agent", "agent host:port").Default("localhost:45327").String()

	reader             = app.Command("read", "Read data from a topic")
	readTopic          = reader.Flag("topic", "Name of a source topic").Required().String()
	readerAgentAddress = reader.Flag("agent", "agent host:port").Default("localhost:45327").String()
)

func main() {

	switch kingpin.MustParse(app.Parse(os.Args[1:])) {

	case master.FullCommand():
		println("listening on", *masterAddress)
		m.RunMaster(nil, *masterAddress)

	case executor.FullCommand():

		rawData, err := ioutil.ReadAll(os.Stdin)
		if err != nil {
			log.Fatalf("failed to read stdin: %v", err)
		}
		instructions := cmd.InstructionSet{}
		if err := proto.Unmarshal(rawData, &instructions); err != nil {
			log.Fatal("unmarshaling instructions error: ", err)
		}
		exe.NewExecutor(nil, &instructions).ExecuteInstructionSet()

	case writer.FullCommand():

		inChan := util.NewPiper()
		var wg sync.WaitGroup
		wg.Add(1)
		go netchan.DialWriteChannel(&wg, "stdin", *writerAgentAddress, *writeTopic, inChan.Reader, 1)
		wg.Add(1)
		go util.LineReaderToChannel(&wg, "stdin", os.Stdin, inChan.Writer, true, os.Stderr)
		wg.Wait()

	case reader.FullCommand():

		outChan := util.NewPiper()
		var wg sync.WaitGroup
		wg.Add(1)
		go netchan.DialReadChannel(&wg, "stdout", *readerAgentAddress, *readTopic, outChan.Writer)
		wg.Add(1)
		util.ChannelToLineWriter(&wg, "stdout", outChan.Reader, os.Stdout, os.Stderr)
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

		if *agentOption.InMemory {
			println("running in memory only mode")
		}
		agentServer := a.NewAgentServer(agentOption)
		agentServer.Run()
	}
}
