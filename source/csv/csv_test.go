package csv

import (
	"testing"
)

func TestEncodingDecoding(t *testing.T) {
	f := &CsvInputFormat{}
	cis := &CsvInputSplit{
		FileName:  "x",
		HasHeader: true,
	}
	data, err := f.EncodeInputSplit(cis)
	if err != nil {
		t.Errorf("failed to encode:%v", err)
	}

	decoded, err := f.DecodeInputSplit(data)
	if err != nil {
		t.Errorf("failed to decode:%v", err)
	}

	if newCis, ok := decoded.(*CsvInputSplit); ok {
		if newCis.FileName != cis.FileName {
			t.Errorf("failed to decode.")
		}
	} else {
		t.Errorf("failed to assert.")
	}

}
