// Package master collects data from agents, and manage named network
// channels.
package master

import (
	"io"
	"log"
	"net"
	"net/http"

	pb "github.com/chrislusf/gleam/idl/master_rpc"
	"github.com/soheilhy/cmux"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var masterServer *MasterServer

func init() {
	masterServer = newMasterServer()
}

func RunMaster(listenOn string) {

	listener, err := net.Listen("tcp", listenOn)
	if err != nil {
		log.Fatalf("master server fails to listen on %s: %v", listenOn, err)
	}
	defer listener.Close()

	m := cmux.New(listener)

	grpcL := m.Match(cmux.HTTP2HeaderField("content-type", "application/grpc"))
	httpL := m.Match(cmux.HTTP1Fast())

	// Create your protocol servers.
	grpcS := grpc.NewServer()
	pb.RegisterGleamMasterServer(grpcS, masterServer)
	reflection.Register(grpcS)

	httpS := &http.Server{Handler: &masterHttpHandler{mux: mux}}

	go grpcS.Serve(grpcL)
	go httpS.Serve(httpL)

	if err := m.Serve(); err != nil {
		log.Fatalf("master server failed to serve: %v", err)
	}

}

type masterHttpHandler struct {
	mux map[string]func(http.ResponseWriter, *http.Request)
}

func (m *masterHttpHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h, ok := m.mux[r.URL.String()]; ok {
		h(w, r)
		return
	}
	io.WriteString(w, "My server: "+r.URL.String())
}
