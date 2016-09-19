// Package agent runs on servers with computing resources, and executes
// tasks sent by driver.
package agent

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	// "net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/chrislusf/gleam/distributed/cmd"
	"github.com/chrislusf/glow/resource"
	// "github.com/chrislusf/glow/resource/service_discovery/client"
	"github.com/chrislusf/gleam/util"
	"github.com/golang/protobuf/proto"
)

type AgentServerOption struct {
	Master       *string
	Host         *string
	Port         *int
	Dir          *string
	DataCenter   *string
	Rack         *string
	MaxExecutor  *int
	MemoryMB     *int64
	CPULevel     *int
	CleanRestart *bool
}

type AgentServer struct {
	Option                *AgentServerOption
	Master                string
	wg                    sync.WaitGroup
	listener              net.Listener
	computeResource       *resource.ComputeResource
	allocatedResource     *resource.ComputeResource
	allocatedResourceLock sync.Mutex
	storageBackend        *LocalDatasetShardsManager
	localExecutorManager  *LocalExecutorManager
}

func NewAgentServer(option *AgentServerOption) *AgentServer {
	absoluteDir, err := filepath.Abs(util.CleanPath(*option.Dir))
	if err != nil {
		panic(err)
	}
	println("starting in", absoluteDir)
	option.Dir = &absoluteDir

	as := &AgentServer{
		Option:         option,
		Master:         *option.Master,
		storageBackend: NewLocalDatasetShardsManager(*option.Dir, *option.Port),
		computeResource: &resource.ComputeResource{
			CPUCount: *option.MaxExecutor,
			CPULevel: *option.CPULevel,
			MemoryMB: *option.MemoryMB,
		},
		allocatedResource:    &resource.ComputeResource{},
		localExecutorManager: newLocalExecutorsManager(),
	}

	err = as.init()
	if err != nil {
		panic(err)
	}

	return as
}

func (r *AgentServer) init() (err error) {
	r.listener, err = net.Listen("tcp", *r.Option.Host+":"+strconv.Itoa(*r.Option.Port))

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("AgentServer starts on", *r.Option.Host+":"+strconv.Itoa(*r.Option.Port))

	if *r.Option.CleanRestart {
		if fileInfos, err := ioutil.ReadDir(r.storageBackend.dir); err == nil {
			suffix := fmt.Sprintf("-%d.dat", *r.Option.Port)
			for _, fi := range fileInfos {
				name := fi.Name()
				if !fi.IsDir() && strings.HasSuffix(name, suffix) {
					// println("removing old dat file:", name)
					os.Remove(filepath.Join(r.storageBackend.dir, name))
				}
			}
		}
	}

	return
}

func (as *AgentServer) Run() {
	//register agent
	/*
		killHeartBeaterChan := make(chan bool, 1)
		go client.NewHeartBeater(*as.Option.Host, *as.Option.Port, as.Master).StartAgentHeartBeat(killHeartBeaterChan, func(values url.Values) {
			resource.AddToValues(values, as.computeResource, as.allocatedResource)
			values.Add("dataCenter", *as.Option.DataCenter)
			values.Add("rack", *as.Option.Rack)
		})
	*/

	for {
		// Listen for an incoming connection.
		conn, err := as.listener.Accept()
		if err != nil {
			fmt.Println("Error accepting: ", err.Error())
			os.Exit(1)
		}
		// Handle connections in a new goroutine.
		as.wg.Add(1)
		go func() {
			defer as.wg.Done()
			defer conn.Close()
			as.handleRequest(conn)
		}()
	}
}

func (r *AgentServer) Stop() {
	r.listener.Close()
	r.wg.Wait()
}

// Handles incoming requests.
func (r *AgentServer) handleRequest(conn net.Conn) {

	data, err := util.ReadMessage(conn)

	/*
		tlscon, ok := conn.(*tls.Conn)
		if ok {
			state := tlscon.ConnectionState()
			if !state.HandshakeComplete {
				log.Printf("Failed to tls handshake with: %+v", tlscon.RemoteAddr())
				return
			}
		}
	*/

	if err != nil {
		log.Printf("Failed to read command %s:%v", err)
		return
	}

	newCmd := &cmd.ControlMessage{}
	if err := proto.Unmarshal(data, newCmd); err != nil {
		log.Fatal("unmarshaling error: ", err)
	}
	reply := r.handleCommandConnection(conn, newCmd)
	if reply != nil {
		data, err := proto.Marshal(reply)
		if err != nil {
			log.Fatal("marshaling error: ", err)
		}
		conn.Write(data)
	}

}

func (as *AgentServer) handleCommandConnection(conn net.Conn,
	command *cmd.ControlMessage) *cmd.ControlMessage {
	reply := &cmd.ControlMessage{}
	if command.GetReadRequest() != nil {
		as.handleReadConnection(conn, *command.ReadRequest.Name)
		return nil
	}
	if command.GetWriteRequest() != nil {
		as.handleLocalWriteConnection(conn, *command.WriteRequest.Name)
		return nil
	}
	if command.GetStartRequest() != nil {
		// println("start from", *command.StartRequest.Host)
		if *command.StartRequest.Host == "" {
			remoteAddress := conn.RemoteAddr().String()
			// println("remote address is", remoteAddress)
			host := remoteAddress[:strings.LastIndex(remoteAddress, ":")]
			command.StartRequest.Host = &host
		}
		reply.StartResponse = as.handleStart(conn, command.StartRequest)
		// return nil to avoid writing the response to the connection.
		// Currently the connection is used for reading outputs
		return nil
	}
	if command.GetDeleteDatasetShardRequest() != nil {
		reply.DeleteDatasetShardResponse = as.handleDeleteDatasetShard(conn, command.DeleteDatasetShardRequest)
	} else if command.GetGetStatusRequest() != nil {
		reply.GetStatusResponse = as.handleGetStatusRequest(command.GetGetStatusRequest())
	} else if command.GetStopRequest() != nil {
		reply.StopResponse = as.handleStopRequest(command.GetStopRequest())
	} else if command.GetLocalStatusReportRequest() != nil {
		reply.LocalStatusReportResponse = as.handleLocalStatusReportRequest(command.GetLocalStatusReportRequest())
	}
	return reply
}
