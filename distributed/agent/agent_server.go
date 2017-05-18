// Package agent runs on servers with computing resources, and executes
// tasks sent by driver.
package agent

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/chrislusf/gleam/pb"
	"github.com/chrislusf/gleam/util"
	"github.com/golang/protobuf/proto"
	"github.com/soheilhy/cmux"
)

type AgentServerOption struct {
	Master       *string
	Host         *string
	Port         *int32
	Dir          *string
	DataCenter   *string
	Rack         *string
	MaxExecutor  *int32
	MemoryMB     *int64
	CPULevel     *int32
	CleanRestart *bool
}

type AgentServer struct {
	Option                  *AgentServerOption
	Master                  string
	computeResource         *pb.ComputeResource
	allocatedResource       *pb.ComputeResource
	allocatedHasChanges     bool
	allocatedResourceLock   sync.Mutex
	storageBackend          *LocalDatasetShardsManager
	inMemoryChannels        *LocalDatasetShardsManagerInMemory
	receiveFileResourceLock sync.Mutex
}

func RunAgentServer(option *AgentServerOption) {
	absoluteDir, err := filepath.Abs(util.CleanPath(*option.Dir))
	if err != nil {
		panic(err)
	}
	println("starting in", absoluteDir)
	option.Dir = &absoluteDir

	as := &AgentServer{
		Option:           option,
		Master:           *option.Master,
		storageBackend:   NewLocalDatasetShardsManager(*option.Dir, int(*option.Port)),
		inMemoryChannels: NewLocalDatasetShardsManagerInMemory(),
		computeResource: &pb.ComputeResource{
			CpuCount: int32(*option.MaxExecutor),
			CpuLevel: int32(*option.CPULevel),
			MemoryMb: *option.MemoryMB,
		},
		allocatedResource: &pb.ComputeResource{},
	}

	go as.storageBackend.purgeExpiredEntries()
	go as.inMemoryChannels.purgeExpiredEntries()
	go as.heartbeat()

	listener, err := net.Listen("tcp", fmt.Sprintf("%v:%d", *option.Host, *option.Port))
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("AgentServer starts on", fmt.Sprintf("%v:%d", *option.Host, *option.Port))

	if *option.CleanRestart {
		if fileInfos, err := ioutil.ReadDir(as.storageBackend.dir); err == nil {
			suffix := fmt.Sprintf("-%d.dat", *option.Port)
			for _, fi := range fileInfos {
				name := fi.Name()
				if !fi.IsDir() && strings.HasSuffix(name, suffix) {
					// println("removing old dat file:", name)
					os.Remove(filepath.Join(as.storageBackend.dir, name))
				}
			}
		}
	}

	m := cmux.New(listener)
	grpcListener := m.Match(cmux.HTTP2HeaderField("content-type", "application/grpc"))
	tcpListener := m.Match(cmux.Any())

	go as.serveGrpc(grpcListener)
	go as.serveTcp(tcpListener)

	if err := m.Serve(); !strings.Contains(err.Error(), "use of closed network connection") {
		panic(err)
	}

}

// Run starts the heartbeating to master and starts accepting requests.
func (as *AgentServer) serveTcp(listener net.Listener) {

	for {
		// Listen for an incoming connection.
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting: ", err.Error())
			continue
		}
		// Handle connections in a new goroutine.
		go func() {
			defer conn.Close()
			if err = conn.SetDeadline(time.Time{}); err != nil {
				fmt.Printf("Failed to set timeout: %v\n", err)
			}
			if c, ok := conn.(*net.TCPConn); ok {
				c.SetKeepAlive(true)
			}
			as.handleRequest(conn)
		}()
	}
}

func (r *AgentServer) handleRequest(conn net.Conn) {

	data, err := util.ReadMessage(conn)

	if err != nil {
		log.Printf("Failed to read command:%v", err)
		return
	}

	newCmd := &pb.ControlMessage{}
	if err := proto.Unmarshal(data, newCmd); err != nil {
		log.Fatal("unmarshaling error: ", err)
	}
	r.handleCommandConnection(conn, newCmd)
}

func (as *AgentServer) handleCommandConnection(conn net.Conn,
	command *pb.ControlMessage) {
	if command.GetReadRequest() != nil {
		if !command.GetIsOnDiskIO() {
			as.handleInMemoryReadConnection(conn, command.ReadRequest.ReaderName, command.ReadRequest.ChannelName)
		} else {
			as.handleReadConnection(conn, command.ReadRequest.ReaderName, command.ReadRequest.ChannelName)
		}
	}
	if command.GetWriteRequest() != nil {
		if !command.GetIsOnDiskIO() {
			as.handleLocalInMemoryWriteConnection(conn, command.WriteRequest.WriterName, command.WriteRequest.ChannelName, int(command.GetWriteRequest().GetReaderCount()))
		} else {
			as.handleLocalWriteConnection(conn, command.WriteRequest.WriterName, command.WriteRequest.ChannelName, int(command.GetWriteRequest().GetReaderCount()))
		}
	}
}
