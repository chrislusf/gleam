// Disk-backed queue
package store

import (
	"io"
	"path"
)

type DataStore interface {
	io.Writer
	io.ReaderAt
	Destroy()
}

type LocalFileDataStore struct {
	dir   string
	name  string
	store *RotatingFileStore
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
	}
	ds.store.init()
	return
}

func (ds *LocalFileDataStore) Write(data []byte) (int, error) {
	count, err := ds.store.Write(data)
	return count, err
}

func (ds *LocalFileDataStore) ReadAt(data []byte, offset int64) (int, error) {
	return ds.store.ReadAt(data, offset)
}

func (ds *LocalFileDataStore) Destroy() {
	ds.store.Destroy()
}
