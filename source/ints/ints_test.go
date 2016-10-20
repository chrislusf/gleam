package ints

import (
	"testing"
)

func TestEncodingDecoding(t *testing.T) {
	f := &IntsInputFormat{}
	cis := &IntsInputSplit{
		Start: 50,
		Stop:  100,
	}
	data, err := f.EncodeInputSplit(cis)
	if err != nil {
		t.Errorf("failed to encode:%v", err)
	}

	decoded, err := f.DecodeInputSplit(data)
	if err != nil {
		t.Errorf("failed to decode:%v", err)
	}

	if newCis, ok := decoded.(*IntsInputSplit); ok {
		if newCis.Start != cis.Start {
			t.Errorf("failed to decode.")
		}
	} else {
		t.Errorf("failed to assert.")
	}

}
