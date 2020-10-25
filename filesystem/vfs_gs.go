package filesystem

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/url"
	"os"
	"strings"

	"cloud.google.com/go/storage"
)

// Contains logic for Google Storage virtual filesystem
// Note that Google Storage authentication expects GOOGLE_APPLICATION_CREDENTIALS
// see: https://cloud.google.com/docs/authentication/getting-started
// Note that this is handled internally for clusters running within Google Cloud

// GoogleStorageFileSystem type
type GoogleStorageFileSystem struct{}

// Accept - criteria for accepting path as google storage location
func (fs *GoogleStorageFileSystem) Accept(fl *FileLocation) bool {
	return strings.HasPrefix(fl.Location, "gs://")
}

// Open - open given file location and return virtual file
func (fs *GoogleStorageFileSystem) Open(fl *FileLocation) (v VirtualFile, err error) {
	ctx := context.Background()
	gs, err := storage.NewClient(ctx)
	if err != nil {
		return
	}

	u, err := url.Parse(fl.Location)
	if err != nil {
		return
	}

	r, err := gs.Bucket(u.Hostname()).Object(u.Path).NewReader(ctx)
	if err != nil {
		return
	}

	return newVirtualFileGS(r)
}

// List - list items in google storage directory
func (fs *GoogleStorageFileSystem) List(fl *FileLocation) (fileLocations []*FileLocation, err error) {
	ctx := context.Background()
	gs, err := storage.NewClient(ctx)
	if err != nil {
		return
	}

	u, err := url.Parse(fl.Location)
	if err != nil {
		return
	}

	bucket := gs.Bucket(u.Hostname())
	iter := bucket.Objects(ctx, &storage.Query{Prefix: u.Path[1:]})

	for {
		attr, err := iter.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return fileLocations, err
		}

		fileLocations = append(fileLocations, &FileLocation{
			Location: fmt.Sprintf("gs://%s/%s", attr.Bucket, attr.Name),
		})
	}

	return
}

// IsDir - returns true if directory detected
// This function assumes that if the prefix ends with "/"
// then the intent of the user is to represent a Dir
func (fs *GoogleStorageFileSystem) IsDir(fl *FileLocation) bool {
	return strings.HasSuffix(fl.Location, "/")
}

// VirtualFileGS - Virtual File implementation for Google Storage
type VirtualFileGS struct {
	*os.File
	filename string
	size     int64
}

// Size - returns virtual file  size
func (vf *VirtualFileGS) Size() int64 {
	return vf.size
}

// Close - virtual file Closeer function
func (vf *VirtualFileGS) Close() error {
	vf.File.Close()
	return os.Remove(vf.filename)
}

func newVirtualFileGS(readerCloser io.ReadCloser) (*VirtualFileGS, error) {
	filename := fmt.Sprintf("%s/gs_%d", os.TempDir(), rand.Uint32())
	outFile, err := os.Create(filename)
	if err != nil {
		return nil, err
	}
	size, err := io.Copy(outFile, readerCloser)
	readerCloser.Close()

	outFile.Seek(0, 0)

	return &VirtualFileGS{outFile, filename, size}, err
}
