package util

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

func TestJson(t *testing.T) {
	writer := httptest.ResponseRecorder{
		Body: &bytes.Buffer{},
	}
	req := http.Request{}
	httpStatus := 200
	obj := struct {
		A int `json:"a"`
		B int `json:"b"`
	}{
		A: 100,
		B: 100,
	}

	want := []byte("a: 100 b: 100")
	if err := Json(&writer, &req, httpStatus, obj); err != nil || reflect.DeepEqual(want, writer.Body.Bytes()) {
		t.Errorf("Got %v, error: %v", writer.Body.Bytes(), err)
	}
}

func TestError(t *testing.T) {
	writer := httptest.ResponseRecorder{
		Body: &bytes.Buffer{},
	}
	req := http.Request{}
	httpStatus := 200

	want, err := json.Marshal(map[string]string{"error": "test"})
	if err != nil {
		t.Fatal(err)
	}
	if err := Error(&writer, &req, httpStatus, "test error"); err != nil || reflect.DeepEqual(want, writer.Body.Bytes()) {
		t.Errorf("Got %v, error: %v", writer.Body.Bytes(), err)
	}
}
