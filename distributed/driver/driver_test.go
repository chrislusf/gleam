package driver

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/chrislusf/gleam/flow"
	"google.golang.org/grpc"
)

func TestConcurrentDriver(t *testing.T) {
	srv := grpc.NewServer()
	l, err := net.Listen("tcp", "0.0.0.0:4321")
	if err != nil {
		t.Fatalf("could not listen: %v", err)
	}
	go func() {
		if err := srv.Serve(l); err != nil {
			l.Close()
			t.Fatalf("could not serve: %v", err)
		}
	}()

	fcd := NewFlowDriver(&Option{})
	go fcd.RunFlowContext(context.Background(), flow.New("test"))

	done := make(chan bool)
	time.AfterFunc(time.Second, func() { close(done) })

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go fcd.reportStatus(ctx, &wg, "0.0.0.0:4321", done)
	wg.Wait()
}
