package main

import (
	"bufio"
	"encoding/binary"
	"log"
	"os"
	"runtime"
	"strconv"
	"sync"

	kingpin "gopkg.in/alecthomas/kingpin.v2"

	a "github.com/chrislusf/gleam/distributed/agent"
	"github.com/chrislusf/gleam/distributed/cmd"
	exe "github.com/chrislusf/gleam/distributed/executor"
	"github.com/chrislusf/gleam/distributed/netchan"
	"github.com/chrislusf/gleam/util"
	"github.com/golang/protobuf/proto"
)

var (
	app = kingpin.New("gleamd", "distributed gleam, acts as master, agent, or executor")

	executor               = app.Command("execute", "Execute an instruction set")
	executorInstructionSet = app.Command("execute.instructions", "The instruction set")

	agent       = app.Command("agent", "Agent that can accept read, write requests, manage executors")
	agentOption = &a.AgentServerOption{
		Dir:          agent.Flag("dir", "agent folder to store computed data").Default(os.TempDir()).String(),
		Host:         agent.Flag("host", "agent listening host address. Required in 2-way SSL mode.").Default("").String(),
		Port:         agent.Flag("port", "agent listening port").Default("45326").Int(),
		Master:       agent.Flag("master", "master address").Default("localhost:8930").String(),
		DataCenter:   agent.Flag("dataCenter", "data center name").Default("defaultDataCenter").String(),
		Rack:         agent.Flag("rack", "rack name").Default("defaultRack").String(),
		MaxExecutor:  agent.Flag("max.executors", "upper limit of executors").Default(strconv.Itoa(runtime.NumCPU())).Int(),
		CPULevel:     agent.Flag("cpu.level", "relative computing power of single cpu core").Default("1").Int(),
		MemoryMB:     agent.Flag("memory", "memory size in MB").Default("1024").Int64(),
		CleanRestart: agent.Flag("clean.restart", "clean up previous dataset files").Default("true").Bool(),
	}

	writer             = app.Command("write", "Write data to a topic")
	writeTopic         = writer.Flag("topic", "Name of a topic").Required().String()
	writerAgentAddress = writer.Flag("agent", "agent host:port").Default("localhost:45326").String()

	reader             = app.Command("read", "Read data from a topic")
	readTopic          = reader.Flag("topic", "Name of a source topic").Required().String()
	readerAgentAddress = reader.Flag("agent", "agent host:port").Default("localhost:45326").String()
)

func main() {

	switch kingpin.MustParse(app.Parse(os.Args[1:])) {

	case executor.FullCommand():

		reader := bufio.NewReader(os.Stdin)
		var dataLen int32
		binary.Read(reader, binary.LittleEndian, dataLen)
		data := make([]byte, dataLen)
		instructions := &cmd.InstructionSet{}
		if err := proto.Unmarshal(data, instructions); err != nil {
			log.Fatal("unmarshaling instructions error: ", err)
		}
		exe.NewExecutor(nil, instructions).ExecuteInstructionSet(nil)

	case writer.FullCommand():

		inChan := make(chan []byte, 16)
		var wg sync.WaitGroup
		wg.Add(1)
		go netchan.DialWriteChannel(&wg, *writerAgentAddress, *writeTopic, inChan)
		wg.Add(1)
		go util.LineReaderToChannel(&wg, "stdin", os.Stdin, inChan, true, os.Stderr)
		wg.Wait()

	case reader.FullCommand():

		outChan := make(chan []byte, 16)
		var wg sync.WaitGroup
		wg.Add(1)
		go netchan.DialReadChannel(&wg, *readerAgentAddress, *readTopic, outChan)
		wg.Add(1)
		util.ChannelToLineWriter(&wg, "stdout", outChan, os.Stdout, os.Stderr)
		wg.Wait()

	case agent.FullCommand():

		agentServer := a.NewAgentServer(agentOption)
		agentServer.Run()
	}
}
