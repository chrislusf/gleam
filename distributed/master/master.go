// Package master collects data from agents, and manage named network
// channels.
package master

import (
	"log"
	"net"
	"net/http"

	"github.com/chrislusf/gleam/pb"
	"github.com/chrislusf/gleam/util"
	"github.com/gorilla/mux"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var masterServer *MasterServer

func RunMaster(listenOn string, logDirectory string) {

	masterServer = newMasterServer(logDirectory)

	httpL, err := net.Listen("tcp", listenOn)
	if err != nil {
		log.Fatalf("master server fails to listen on %s: %v", listenOn, err)
	}
	defer httpL.Close()

	grpcAddress, err := util.ParseServerToGrpcAddress(listenOn)
	if err != nil {
		log.Fatalf("master server fails to parse listen on %s: %v", listenOn, err)
	}
	grpcL, err := net.Listen("tcp", grpcAddress)
	if err != nil {
		log.Fatalf("master server fails to listen on %s: %v", listenOn, err)
	}
	defer grpcL.Close()

	// Create your protocol servers.
	grpcS := grpc.NewServer()
	pb.RegisterGleamMasterServer(grpcS, masterServer)
	reflection.Register(grpcS)

	r := mux.NewRouter()
	r.HandleFunc("/job/{id:[0-9]+}", masterServer.jobStatusHandler)
	r.HandleFunc("/", masterServer.uiStatusHandler)

	go grpcS.Serve(grpcL)
	go http.Serve(httpL, r)

	select {}

}
