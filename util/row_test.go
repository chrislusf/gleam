package util

import (
	"testing"
)

func TestMap(t *testing.T) {
	// m := make(map[string][]interface{})
	m := make(map[string]interface{})
	m["def"] = []float64{1.2, 3.4}

	row := Row{}
	row.K = []interface{}{"abc"}
	row.V = []interface{}{m}
	encoded, err := encodeRow(row)
	t.Logf("encoded row: %x", encoded)
	if err != nil {
		t.Fatal(err)
	}
}
