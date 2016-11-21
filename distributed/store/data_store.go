// Disk-backed queue
package store

import (
	"io"
	"path"
	"time"
)

type DataStore interface {
	io.Writer
	io.ReaderAt
	Destroy()
	LastWriteAt() time.Time
	LastReadAt() time.Time
}

type LocalFileDataStore struct {
	dir         string
	name        string
	store       *RotatingFileStore
	lastWriteAt time.Time
	lastReadAt  time.Time
}

func NewLocalFileDataStore(dir, name string) (ds *LocalFileDataStore) {
	ds = &LocalFileDataStore{
		dir:  dir,
		name: name,
		store: &RotatingFileStore{
			Filename:    path.Join(dir, name+".dat"),
			MaxMegaByte: 256,
			MaxDays:     0, // unlimited
			MaxBackups:  0, // unlimited
			LocalTime:   true,
		},
		lastWriteAt: time.Now(),
	}
	ds.store.init()
	return
}

func (ds *LocalFileDataStore) Write(data []byte) (int, error) {
	count, err := ds.store.Write(data)
	ds.lastWriteAt = time.Now()
	return count, err
}

func (ds *LocalFileDataStore) ReadAt(data []byte, offset int64) (int, error) {
	ds.lastReadAt = time.Now()
	return ds.store.ReadAt(data, offset)
}

func (ds *LocalFileDataStore) Destroy() {
	ds.store.Destroy()
}

func (ds *LocalFileDataStore) LastWriteAt() time.Time {
	return ds.lastWriteAt
}

func (ds *LocalFileDataStore) LastReadAt() time.Time {
	return ds.lastReadAt
}
