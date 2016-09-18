package store

// adapted from https://github.com/natefinch/lumberjack
import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	backupTimeFormat   = "2006-01-02T15-04-05.000"
	defaultMaxMegaByte = 100
)

type Segment struct {
	Offset int64
	Size   int64
	File   *os.File
	mu     sync.Mutex
}

type RotatingFileStore struct {
	// Filename is the file to write logs to.  Backup log files will be retained
	// in the same directory.
	Filename string

	// MaxMegaByte is the maximum size in megabytes of the log file before it gets
	// rotated. It defaults to 100 megabytes.
	MaxMegaByte int

	// MaxDays is the maximum number of days to retain old log files based on the
	// timestamp encoded in their filename.  Note that a day is defined as 24
	// hours and may not exactly correspond to calendar days due to daylight
	// savings, leap seconds, etc. The default is not to remove old log files
	// based on age.
	MaxDays int

	// MaxBackups is the maximum number of old log files to retain.  The default
	// is to retain all old log files (though MaxDays may still cause them to get
	// deleted.)
	MaxBackups int

	// LocalTime determines if the time used for formatting the timestamps in
	// backup files is the computer's local time.  The default is to use UTC
	// time.
	LocalTime bool

	size           int64
	file           *os.File
	mu             sync.Mutex
	waitForReading *sync.Cond

	Segments []*Segment
	Offset   int64 // offset at the head of the file
	Position int64 // offset for current tail, write position
}

func (l *RotatingFileStore) init() {
	l.waitForReading = sync.NewCond(&l.mu)
}

var (
	// currentTime exists so it can be mocked out by tests.
	currentTime = time.Now

	// os_Stat exists so it can be mocked out by tests.
	os_Stat = os.Stat

	// megabyte is the conversion factor between MaxMegaByte and bytes.  It is a
	// variable so tests can mock it out and not need to write megabytes of data
	// to disk.
	megabyte = 1024 * 1024
)

func (l *RotatingFileStore) ReadAt(data []byte, offset int64) (int, error) {
	if offset < l.Offset {
		return l.readFullFromOldSegmentsAt(data, offset)
	}

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

func (l *RotatingFileStore) readFullFromOldSegmentsAt(data []byte, offset int64) (int, error) {
	index := len(l.Segments) - 1
	for i := 1; i < len(l.Segments); i++ {
		if offset < l.Segments[i].Offset {
			index = i - 1
		}
	}
	file := l.Segments[index].File
	return file.ReadAt(data, offset-l.Segments[index].Offset)
}

// Write implements io.Writer.  If a write would cause the log file to be larger
// than MaxMegaByte, the file is closed, renamed to include a timestamp of the
// current time, and a new log file is created using the original log file name.
// If the length of the write is greater than MaxMegaByte, an error is returned.
func (l *RotatingFileStore) Write(p []byte) (n int, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	writeLen := int64(len(p))
	if writeLen > l.max() {
		return 0, fmt.Errorf(
			"write length %d exceeds maximum file size %d", writeLen, l.max(),
		)
	}

	if l.file == nil {
		if err = l.openNew(); err != nil {
			return 0, err
		}
	}

	if l.size+writeLen > l.max() {
		if err := l.rotate(); err != nil {
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
func (l *RotatingFileStore) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.close()
}

// close closes the file if it is open.
func (l *RotatingFileStore) close() error {
	if l.file == nil {
		return nil
	}
	err := l.file.Close()
	l.file = nil
	return err
}

// Rotate causes RotatingFileStore to close the existing log file and immediately create a
// new one.  This is a helper function for applications that want to initiate
// rotations outside of the normal rotation rules, such as in response to
// SIGHUP.  After rotating, this initiates a cleanup of old log files according
// to the normal rules.
func (l *RotatingFileStore) Rotate() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.rotate()
}

// rotate closes the current file, moves it aside with a timestamp in the name,
// (if it exists), opens a new file with the original filename, and then runs
// cleanup.
func (l *RotatingFileStore) rotate() error {
	if err := l.close(); err != nil {
		return err
	}

	if err := l.openNew(); err != nil {
		return err
	}
	return l.cleanup()
}

// openNew opens a new log file for writing, moving any old log file out of the
// way.  This methods assumes the file has already been closed.
func (l *RotatingFileStore) openNew() error {
	err := os.MkdirAll(l.dir(), 0744)
	if err != nil {
		return fmt.Errorf("can't make directories for new logfile: %s", err)
	}

	name := l.filename()
	mode := os.FileMode(0644)
	info, err := os_Stat(name)
	if err == nil {
		// Copy the mode off the old logfile.
		mode = info.Mode()
		// move the existing file
		newname := backupName(name, l.LocalTime)
		if err := os.Rename(name, newname); err != nil {
			return fmt.Errorf("can't rename log file: %s", err)
		}
		l.Offset += info.Size()
	}

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

// backupName creates a new filename from the given name, inserting a timestamp
// between the filename and the extension, using the local time if requested
// (otherwise UTC).
func backupName(name string, local bool) string {
	dir := filepath.Dir(name)
	filename := filepath.Base(name)
	ext := filepath.Ext(filename)
	prefix := filename[:len(filename)-len(ext)]
	t := currentTime()
	if !local {
		t = t.UTC()
	}

	timestamp := t.Format(backupTimeFormat)
	return filepath.Join(dir, fmt.Sprintf("%s-%s%s", prefix, timestamp, ext))
}

// THIS IS NOT USED!
// openExistingOrNew opens the logfile if it exists and if the current write
// would not put it over MaxMegaByte.  If there is no such file or the write would
// put it over the MaxMegaByte, a new file is created.
func (l *RotatingFileStore) openExistingOrNew(writeLen int) error {
	filename := l.filename()
	info, err := os_Stat(filename)
	if os.IsNotExist(err) {
		return l.openNew()
	}
	if err != nil {
		return fmt.Errorf("error getting log file info: %s", err)
	}

	if info.Size()+int64(writeLen) >= l.max() {
		return l.rotate()
	}

	file, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		// if we fail to open the old log file for some reason, just ignore
		// it and open a new log file.
		return l.openNew()
	}
	l.file = file
	l.size = info.Size()
	return nil
}

func (l *RotatingFileStore) filename() string {
	return l.Filename
}

// cleanup deletes old log files, keeping at most l.MaxBackups files, as long as
// none of them are older than MaxDays.
func (l *RotatingFileStore) cleanup() error {
	if l.MaxBackups == 0 && l.MaxDays == 0 {
		return nil
	}

	files, err := l.listOldLogFiles()
	if err != nil {
		return err
	}

	var deletes []logInfo

	if l.MaxBackups > 0 && l.MaxBackups < len(files) {
		deletes = files[:l.MaxBackups]
		files = files[l.MaxBackups:]
	}
	if l.MaxDays > 0 {
		diff := time.Duration(int64(24*time.Hour) * int64(l.MaxDays))

		cutoff := currentTime().Add(-1 * diff)

		for _, f := range files {
			if f.timestamp.Before(cutoff) {
				deletes = append(deletes, f)
			}
		}
	}

	if len(deletes) == 0 {
		return nil
	}

	deleteAll(l.dir(), deletes)

	return nil
}

func deleteAll(dir string, files []logInfo) {
	for _, f := range files {
		_ = os.Remove(filepath.Join(dir, f.Name()))
	}
}

func (l *RotatingFileStore) Destroy() {
	for _, seg := range l.Segments {
		// println("removing segment:", seg.File.Name())
		seg.File.Close()
		os.Remove(seg.File.Name())
	}
	// println("removing file", l.filename())
	l.Close()
	os.Remove(l.filename())
}

// oldLogFiles returns the list of backup log files stored in the same
// directory as the current log file, sorted by ModTime
func (l *RotatingFileStore) listOldLogFiles() ([]logInfo, error) {
	os.MkdirAll(l.dir(), 0755)
	files, err := ioutil.ReadDir(l.dir())
	if err != nil {
		return nil, fmt.Errorf("can't read log file directory: %s", err)
	}
	logFiles := []logInfo{}

	prefix, ext := l.prefixAndExt()

	for _, f := range files {
		if f.IsDir() {
			continue
		}
		name := l.timeFromName(f.Name(), prefix, ext)
		if name == "" {
			continue
		}
		t, err := time.Parse(backupTimeFormat, name)
		if err == nil {
			logFiles = append(logFiles, logInfo{t, f})
		}
		// error parsing means that the suffix at the end was not generated
		// by lumberjack, and therefore it's not a backup file.
	}

	sort.Sort(byFormatTime(logFiles))

	return logFiles, nil
}

// timeFromName extracts the formatted time from the filename by stripping off
// the filename's prefix and extension. This prevents someone's filename from
// confusing time.parse.
func (l *RotatingFileStore) timeFromName(filename, prefix, ext string) string {
	if !strings.HasPrefix(filename, prefix) {
		return ""
	}
	filename = filename[len(prefix):]

	if !strings.HasSuffix(filename, ext) {
		return ""
	}
	filename = filename[:len(filename)-len(ext)]
	return filename
}

// max returns the maximum size in bytes of log files before rolling.
func (l *RotatingFileStore) max() int64 {
	if l.MaxMegaByte == 0 {
		return int64(defaultMaxMegaByte * megabyte)
	}
	return int64(l.MaxMegaByte) * int64(megabyte)
}

// dir returns the directory for the current filename.
func (l *RotatingFileStore) dir() string {
	return filepath.Dir(l.filename())
}

// prefixAndExt returns the filename part and extension part from the RotatingFileStore's
// filename.
func (l *RotatingFileStore) prefixAndExt() (prefix, ext string) {
	filename := filepath.Base(l.filename())
	ext = filepath.Ext(filename)
	prefix = filename[:len(filename)-len(ext)] + "-"
	return prefix, ext
}

// logInfo is a convenience struct to return the filename and its embedded
// timestamp.
type logInfo struct {
	timestamp time.Time
	os.FileInfo
}

// byFormatTime sorts by newest time formatted in the name.
type byFormatTime []logInfo

func (b byFormatTime) Less(i, j int) bool {
	return b[i].timestamp.Before(b[j].timestamp)
}

func (b byFormatTime) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}

func (b byFormatTime) Len() int {
	return len(b)
}
