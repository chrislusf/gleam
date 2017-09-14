package util

import (
	"testing"
)

func TestMap(t *testing.T) {
	m := make(map[string][]float64)
	// m := make(map[string]interface{})
	m["def"] = []float64{1.2, 3.4}

	row := Row{}
	row.K = []interface{}{"abc"}
	row.V = []interface{}{m}
	encoded, err := encodeRow(row)
	t.Logf("encoded row of map: %x", encoded)
	if err != nil {
		t.Fatal(err)
	}
}

func TestNumber(t *testing.T) {
	var x uint32
	x = 123

	row := Row{}
	row.K = []interface{}{"abc"}
	row.V = []interface{}{x}
	encoded, err := encodeRow(row)

	t.Logf("encoded row of uint32: %x", encoded)

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
