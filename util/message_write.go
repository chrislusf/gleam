package util

import (
	"encoding/binary"
	"fmt"
	"io"
)

func WriteEOFMessage(writer io.Writer) (err error) {
	if err = binary.Write(writer, binary.LittleEndian, int32(MessageControlEOF)); err != nil {
		return fmt.Errorf("Failed to write message length: %v", err)
	}
	return
}

func WriteMessage(writer io.Writer, m []byte) (err error) {
	if err = binary.Write(writer, binary.LittleEndian, int32(len(m))); err != nil {
		return fmt.Errorf("Failed to write message length: %v", err)
	}
	if _, err = writer.Write(m); err != nil {
		return fmt.Errorf("Failed to write message content: %v", err)
	}
	return
}

type BufferedMessageWriter struct {
	err error
	buf []byte
	n   int
	wr  io.Writer
}

func NewBufferedMessageWriter(w io.Writer, size int) *BufferedMessageWriter {
	return &BufferedMessageWriter{
		buf: make([]byte, size),
		wr:  w,
	}
}

func (b *BufferedMessageWriter) Available() int { return len(b.buf) - b.n }
func (b *BufferedMessageWriter) Buffered() int  { return b.n }

func (b *BufferedMessageWriter) WriteMessage(m []byte) (err error) {
	nextSize := 4 + len(m)
	if nextSize > b.Available() {
		if b.Buffered() > 0 {
			b.flush()
		}
		if nextSize > b.Available() {
			// Large write, empty buffer.
			// Write directly from p to avoid copy.
			return WriteMessage(b.wr, m)
		}
	}
	binary.LittleEndian.PutUint32(b.buf[b.n:], uint32(len(m)))
	n := copy(b.buf[b.n+4:], m)
	b.n += n + 4
	return nil
}

func (b *BufferedMessageWriter) Flush() error {
	err := b.flush()
	return err
}

func (b *BufferedMessageWriter) flush() error {
	if b.err != nil {
		return b.err
	}
	if b.n == 0 {
		return nil
	}
	n, err := b.wr.Write(b.buf[0:b.n])
	if n < b.n && err == nil {
		err = io.ErrShortWrite
	}
	if err != nil {
		if n > 0 && n < b.n {
			copy(b.buf[0:b.n-n], b.buf[n:b.n])
		}
		b.n -= n
		b.err = err
		return err
	}
	b.n = 0
	return nil
}
