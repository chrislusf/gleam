package rsync

import (
	"fmt"
	"os"
	"testing"
	"time"
)

func TestNormalHttpCopy(t *testing.T) {

	files := []string{
		"http_server.go", "./test/some_text.txt", "fetch_url.go",
	}

	rsyncServer, err := NewRsyncServer(files...)
	if err != nil {
		t.Fatalf("Failed to start local server: %v", err)
	}
	rsyncServer.StartRsyncServer(":0")

	err = FetchFilesTo(fmt.Sprintf("localhost:%d", rsyncServer.Port), "/tmp")
	if err != nil {
		fmt.Printf("pausing localhost:%d\n", rsyncServer.Port)
		t.Fatalf("Failed to download file: %v", err)
		time.Sleep(time.Minute)
	}

	os.Remove("/tmp/test/some_text.txt")
	os.Remove("/tmp/fetch_url.go")

	t.Logf("file downloading works ok.")
}
