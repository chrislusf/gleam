package util

import (
	"io"
)

type Piper struct {
	Reader  *io.PipeReader
	Writer  *io.PipeWriter
	Counter int64
	Error   error
}

func NewPiper() *Piper {
	pr, pw := io.Pipe()
	return &Piper{
		Reader: pr,
		Writer: pw,
	}
}
