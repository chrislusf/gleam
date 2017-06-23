package util

import (
	"testing"
)

func TestEncodeDecode(t *testing.T) {

	originalKey := 2349234

	originalData := []interface{}{
		originalKey,
		"jkjkj",
		123,
		"zx,mcv",
	}

	encodedRow, _ := EncodeRow(Now(), originalData...)

	ts, decodedData, _ := DecodeRow(encodedRow)

	println("decoded time:", ts)

	if key, ok := decodedData[0].(int); ok {
		if key != originalKey {
			t.Errorf("Failed to decode key %d => %d", originalKey, key)
		}
	}

	if len(decodedData) != len(originalData) {
		t.Errorf("Failed to decode to the same length %d => %d", len(originalData), len(decodedData))
	}

}
