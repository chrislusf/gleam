package util

import (
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"net/url"
	"testing"
)

func TestSetupHttpClient(t *testing.T) {
	SetupHttpClient(nil)
	if want := "http://"; SchemePrefix != want {
		t.Errorf("SchemePrefix is wrong, got: %s, want %s", SchemePrefix, want)
	}

	SetupHttpClient(&tls.Config{})
	if want := "https://"; SchemePrefix != want {
		t.Errorf("SchemePrefix is wrong, got: %s, want %s", SchemePrefix, want)
	}
}

// Starts a http server to serve '/' with the input handler. Returns the listener or any error.
func listenAndServeAtRoot(handler http.HandlerFunc, t *testing.T) net.Listener {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		serveMux := http.NewServeMux()
		serveMux.HandleFunc("/", handler)
		err := http.Serve(listener, serveMux)
		if err != nil {
			t.Fatal(err)
		}
	}()
	return listener
}

func TestPost(t *testing.T) {
	SetupHttpClient(nil)

	listner := listenAndServeAtRoot(func(w http.ResponseWriter, r *http.Request) {
		// Travis CI sometimes reports build failure:
		// undefined: http.MethodPost
		// So use an string comparison directly.
		if r.Method == "POST" {
			io.WriteString(w, "got post")
		}
	}, t)

	addr := SchemePrefix + listner.Addr().String()
	if got, err := Post(addr, url.Values{}); string(got) != "got post" || err != nil {
		t.Errorf("got: %v, error: %v", got, err)
	}
}

func TestGet(t *testing.T) {
	SetupHttpClient(nil)

	listner := listenAndServeAtRoot(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "test")
	}, t)

	addr := SchemePrefix + listner.Addr().String()
	if got, err := Get(addr); string(got) != "test" || err != nil {
		t.Errorf("got: %v, error: %v", got, err)
	}
}

func TestDownloadUrl(t *testing.T) {
	SetupHttpClient(nil)

	listner := listenAndServeAtRoot(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "some content")
	}, t)

	addr := SchemePrefix + listner.Addr().String()
	if filename, got, err := DownloadUrl(addr); string(got) != "some content" || err != nil {
		t.Errorf("filename: %s, got: %v, error: %v", filename, got, err)
	}
}
