// Package master collects data from agents, and manage named network
// channels.
package master

import (
	"log"
	"net"
	"net/http"

	"github.com/chrislusf/gleam/pb"
	router "github.com/gorilla/mux"
	"github.com/soheilhy/cmux"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var masterServer *MasterServer

func RunMaster(listenOn string, logDirectory string) {

	masterServer = newMasterServer(logDirectory)

	listener, err := net.Listen("tcp", listenOn)
	if err != nil {
		log.Fatalf("master server fails to listen on %s: %v", listenOn, err)
	}
	defer listener.Close()

	m := cmux.New(listener)

	grpcL := m.Match(cmux.HTTP2HeaderField("content-type", "application/grpc"))
	httpL := m.Match(cmux.Any())

	// Create your protocol servers.
	grpcS := grpc.NewServer()
	pb.RegisterGleamMasterServer(grpcS, masterServer)
	reflection.Register(grpcS)

	r := router.NewRouter()
	r.HandleFunc("/", masterServer.uiStatusHandler)
	r.HandleFunc("/job/{id:[0-9]+}", masterServer.jobStatusHandler)
	httpS := &http.Server{Handler: r}

	go grpcS.Serve(grpcL)
	go httpS.Serve(httpL)

	if err := m.Serve(); err != nil {
		log.Fatalf("master server failed to serve: %v", err)
	}

}
