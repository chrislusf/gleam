package util

import (
	"bytes"
	"fmt"
	"testing"

	"gopkg.in/vmihailenco/msgpack.v2"
)

func TestMap(t *testing.T) {
	m := make(map[string][]float64)
	// m := make(map[string]interface{})
	m["def"] = []float64{1.2, 3.4}

	row := Row{}
	row.K = []interface{}{"abc"}
	row.V = []interface{}{m}
	encoded, err := encodeRow(row)
	encodedMsgpack, err := encodeToMsgpack(row)
	t.Logf("encoded grnpack row of map: %x", encoded)
	t.Logf("encoded msgpack row of map: %x", encodedMsgpack)
	if err != nil {
		t.Fatal(err)
	}
}

func TestNumber(t *testing.T) {
	var x uint32
	x = 123

	row := Row{}
	row.K = []interface{}{"abc"}
	row.V = []interface{}{x, 34, 45, 56, 7, 8, 456346, 5634563, 456345634, 568657}
	encoded, err := encodeRow(row)
	encodedMsgpack, err := encodeToMsgpack(row)

	t.Logf("encoded grnpack row of uint32: %x", encoded)
	t.Logf("encoded msgpack row of uint32: %x", encodedMsgpack)

	if err != nil {
		t.Fatal(err)
	}

	var r2 *Row

	r2, err = DecodeRow(encoded)
	if err != nil {
		t.Fatal(err)
	}

	if y, ok := r2.V[0].(uint32); ok {
		if y != x {
			t.Fatalf("Failed to decode uint32 %v", x)
		}
	}

}

func encodeToMsgpack(row Row) ([]byte, error) {
	var buf bytes.Buffer
	encoder := msgpack.NewEncoder(&buf)
	if err := encoder.Encode(row); err != nil {
		return nil, fmt.Errorf("Failed to encode row: %v", err)
	}
	return buf.Bytes(), nil
}
