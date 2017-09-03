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

	originalTs := Now()

	encodedRow, _ := encodeRow(*NewRow(originalTs, originalData...))

	t.Logf("encoded: %x", encodedRow)

	row, _ := DecodeRow(encodedRow)

	if originalTs != row.T {
		t.Errorf("Failed to decode ts %d => %d", originalTs, row.T)
	}

	if key, ok := row.K[0].(int); ok {
		if key != originalKey {
			t.Errorf("Failed to decode key %d => %d", originalKey, key)
		}
	}

	if len(row.V) != len(originalData)-1 {
		t.Errorf("Failed to decode to the same length %d => %d", len(originalData), len(row.V)-1)
	}

}
