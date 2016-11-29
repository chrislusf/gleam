package store

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type SingleFileStore struct {
	// Filename is the file to write logs to.  Backup log files will be retained
	// in the same directory.
	Filename string

	size           int64
	file           *os.File
	mu             sync.Mutex
	waitForReading *sync.Cond

	Offset   int64 // offset at the head of the file
	Position int64 // offset for current tail, write position
}

func (l *SingleFileStore) init() {
	l.waitForReading = sync.NewCond(&l.mu)
}

func (l *SingleFileStore) ReadAt(data []byte, offset int64) (int, error) {

	// fmt.Printf("Read: l.Offset=%d, offset=%d\n", l.Offset, offset)

	l.mu.Lock()
	defer l.mu.Unlock()

	// create the file does not exist
	if l.file == nil {
		// fmt.Printf("Read: creating new file...\n")
		if err := l.openNew(); err != nil {
			return 0, err
		}
	}

	// read written data
	if l.Offset <= offset && offset < l.Position {
		return l.file.ReadAt(data, offset-l.Offset)
	}

	// wait for data not written yet
	for offset == l.Position {
		// fmt.Printf("Read: wait for reading...\n")
		l.waitForReading.Wait()
	}

	// fmt.Printf("Read: file reading...\n")
	return l.file.ReadAt(data, offset-l.Offset)
}

// Write implements io.Writer.  If a write would cause the log file to be larger
// than MaxMegaByte, the file is closed, renamed to include a timestamp of the
// current time, and a new log file is created using the original log file name.
// If the length of the write is greater than MaxMegaByte, an error is returned.
func (l *SingleFileStore) Write(p []byte) (n int, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.file == nil {
		if err = l.openNew(); err != nil {
			return 0, err
		}
	}

	l.file.Seek(0, 2)
	n, err = l.file.Write(p)
	l.size += int64(n)
	l.Position += int64(n)
	l.waitForReading.Broadcast()

	return n, err
}

// Close implements io.Closer, and closes the current logfile.
func (l *SingleFileStore) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.close()
}

// close closes the file if it is open.
func (l *SingleFileStore) close() error {
	if l.file == nil {
		return nil
	}
	err := l.file.Close()
	l.file = nil
	return err
}

// openNew opens a new log file for writing, moving any old log file out of the
// way.  This methods assumes the file has already been closed.
func (l *SingleFileStore) openNew() error {
	err := os.MkdirAll(l.dir(), 0744)
	if err != nil {
		return fmt.Errorf("can't make directories for new logfile: %s", err)
	}

	name := l.filename()
	mode := os.FileMode(0644)

	// we use truncate here because this should only get called when we've moved
	// the file ourselves. if someone else creates the file in the meantime,
	// just wipe out the contents.
	f, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR|os.O_TRUNC, mode)
	if err != nil {
		return fmt.Errorf("can't open new logfile: %s", err)
	}

	l.file = f
	l.size = 0
	return nil
}

func (l *SingleFileStore) filename() string {
	return l.Filename
}

func (l *SingleFileStore) Destroy() {
	// println("removing file", l.filename())
	l.Close()
	os.Remove(l.filename())
}

// dir returns the directory for the current filename.
func (l *SingleFileStore) dir() string {
	return filepath.Dir(l.filename())
}
