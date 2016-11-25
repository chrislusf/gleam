package util

import (
	"bufio"
	"io"
)

// BufWrites ensures all writers are bufio.Writer
// For any bufio.Writer created here, flush it before returning.
func BufWrites(rawWriters []io.Writer, funciton func([]io.Writer)) {
	var writers []io.Writer
	var bufWriters []*bufio.Writer
	for _, w := range rawWriters {
		if bufWriter, ok := w.(*bufio.Writer); ok {
			writers = append(writers, bufWriter)
		} else {
			bufWriter = bufio.NewWriter(w)
			bufWriters = append(bufWriters, bufWriter)
			writers = append(writers, bufWriter)
		}
	}

	funciton(writers)

	for _, w := range bufWriters {
		w.Flush()
	}

}
