package util

import (
	"testing"
)

func TestEncodeDecode(t *testing.T) {

	originalKey := uint32(2349234)

	originalData := []interface{}{
		originalKey,
		"",
		123,
		"zx,mcv",
	}

	originalTs := Now()

	encodedRow, _ := encodeRow(*NewRow(originalTs, originalData...))

	t.Logf("encoded: %x", encodedRow)

	row, _ := DecodeRow(encodedRow)

	if originalTs != row.T {
		t.Errorf("Failed to decode ts %d => %d", originalTs, row.T)
	}

	if key, ok := row.K[0].(uint32); ok {
		if key != originalKey {
			t.Errorf("Failed to decode key %d => %d", originalKey, key)
		}
	} else {
		t.Errorf("Failed to get matching key type %d => %d", originalKey, key)
	}

	if len(row.V) != len(originalData)-1 {
		t.Errorf("Failed to decode to the same length %d => %d", len(originalData), len(row.V)-1)
	}

}
