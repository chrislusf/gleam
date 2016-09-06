package util

import (
	"encoding/json"
	"io"
)

type indentedJSONEncoder struct {
	w io.Writer
}

func (e *indentedJSONEncoder) Encode(v interface{}) error {
	if data, err := json.MarshalIndent(v, "", "  "); err == nil {
		if _, err := e.w.Write(data); err != nil {
			return err
		}
	} else {
		return err
	}

	return nil
}

func NewJSONEncoder(w io.Writer, indent bool) Encoder {
	if indent {
		return &indentedJSONEncoder{w}
	} else {
		return json.NewEncoder(w)
	}
}
