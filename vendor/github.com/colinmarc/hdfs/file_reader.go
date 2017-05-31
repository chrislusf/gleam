package hdfs

import (
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"os"

	hdfs "github.com/colinmarc/hdfs/protocol/hadoop_hdfs"
	"github.com/colinmarc/hdfs/rpc"
	"github.com/golang/protobuf/proto"
)

// A FileReader represents an existing file or directory in HDFS. It implements
// io.Reader, io.ReaderAt, io.Seeker, and io.Closer, and can only be used for
// reads. For writes, see FileWriter and Client.Create.
type FileReader struct {
	client *Client
	name   string
	info   os.FileInfo

	blocks      []*hdfs.LocatedBlockProto
	blockReader *rpc.BlockReader
	offset      int64

	readdirLast string

	closed bool
}

// Open returns an FileReader which can be used for reading.
func (c *Client) Open(name string) (*FileReader, error) {
	info, err := c.getFileInfo(name)
	if err != nil {
		return nil, &os.PathError{"open", name, err}
	}

	return &FileReader{
		client: c,
		name:   name,
		info:   info,
		closed: false,
	}, nil
}

// Name returns the name of the file.
func (f *FileReader) Name() string {
	return f.info.Name()
}

// Stat returns the FileInfo structure describing file.
func (f *FileReader) Stat() os.FileInfo {
	return f.info
}

// Checksum returns HDFS's internal "MD5MD5CRC32C" checksum for a given file.
//
// Internally to HDFS, it works by calculating the MD5 of all the CRCs (which
// are stored alongside the data) for each block, and then calculating the MD5
// of all of those.
func (f *FileReader) Checksum() ([]byte, error) {
	if f.info.IsDir() {
		return nil, &os.PathError{
			"checksum",
			f.name,
			errors.New("is a directory"),
		}
	}

	if f.blocks == nil {
		err := f.getBlocks()
		if err != nil {
			return nil, err
		}
	}

	// The way the hadoop code calculates this, it writes all the checksums out to
	// a byte array, which is automatically padded with zeroes out to the next
	// power of 2 (with a minimum of 32)... and then takes the MD5 of that array,
	// including the zeroes. This is pretty shady business, but we want to track
	// the 'hadoop fs -checksum' behavior if possible.
	paddedLength := 32
	totalLength := 0
	checksum := md5.New()
	for _, block := range f.blocks {
		cr := rpc.NewChecksumReader(block)

		blockChecksum, err := cr.ReadChecksum()
		if err != nil {
			return nil, err
		}

		checksum.Write(blockChecksum)
		totalLength += len(blockChecksum)
		if paddedLength < totalLength {
			paddedLength *= 2
		}
	}

	checksum.Write(make([]byte, paddedLength-totalLength))
	return checksum.Sum(nil), nil
}

// Seek implements io.Seeker.
//
// The seek is virtual - it starts a new block read at the new position.
func (f *FileReader) Seek(offset int64, whence int) (int64, error) {
	if f.closed {
		return 0, io.ErrClosedPipe
	}

	var off int64
	if whence == 0 {
		off = offset
	} else if whence == 1 {
		off = f.offset + offset
	} else if whence == 2 {
		off = f.info.Size() + offset
	} else {
		return f.offset, fmt.Errorf("Invalid whence: %d", whence)
	}

	if off < 0 || off > f.info.Size() {
		return f.offset, fmt.Errorf("Invalid resulting offset: %d", off)
	}

	if f.offset != off {
		f.offset = off
		if f.blockReader != nil {
			f.blockReader.Close()
			f.blockReader = nil
		}
	}
	return f.offset, nil
}

// Read implements io.Reader.
func (f *FileReader) Read(b []byte) (int, error) {
	if f.closed {
		return 0, io.ErrClosedPipe
	}

	if f.info.IsDir() {
		return 0, &os.PathError{
			"read",
			f.name,
			errors.New("is a directory"),
		}
	}

	if f.offset >= f.info.Size() {
		return 0, io.EOF
	}

	if f.blocks == nil {
		err := f.getBlocks()
		if err != nil {
			return 0, err
		}
	}

	if f.blockReader == nil {
		err := f.getNewBlockReader()
		if err != nil {
			return 0, err
		}
	}

	for {
		n, err := f.blockReader.Read(b)
		f.offset += int64(n)

		if err != nil && err != io.EOF {
			f.blockReader.Close()
			f.blockReader = nil
			return n, err
		} else if n > 0 {
			return n, nil
		} else {
			f.blockReader.Close()
			f.getNewBlockReader()
		}
	}
}

// ReadAt implements io.ReaderAt.
func (f *FileReader) ReadAt(b []byte, off int64) (int, error) {
	if f.closed {
		return 0, io.ErrClosedPipe
	}

	_, err := f.Seek(off, 0)
	if err != nil {
		return 0, err
	}

	return io.ReadFull(f, b)
}

// Readdir reads the contents of the directory associated with file and returns
// a slice of up to n os.FileInfo values, as would be returned by Stat, in
// directory order. Subsequent calls on the same file will yield further
// os.FileInfos.
//
// If n > 0, Readdir returns at most n os.FileInfo values. In this case, if
// Readdir returns an empty slice, it will return a non-nil error explaining
// why. At the end of a directory, the error is io.EOF.
//
// If n <= 0, Readdir returns all the os.FileInfo from the directory in a single
// slice. In this case, if Readdir succeeds (reads all the way to the end of
// the directory), it returns the slice and a nil error. If it encounters an
// error before the end of the directory, Readdir returns the os.FileInfo read
// until that point and a non-nil error.
func (f *FileReader) Readdir(n int) ([]os.FileInfo, error) {
	if f.closed {
		return nil, io.ErrClosedPipe
	}

	if !f.info.IsDir() {
		return nil, &os.PathError{
			"readdir",
			f.name,
			errors.New("the file is not a directory"),
		}
	}

	if n <= 0 {
		f.readdirLast = ""
	}

	res, err := f.client.getDirList(f.name, f.readdirLast, n)
	if err != nil {
		return res, err
	}

	if n > 0 {
		if len(res) == 0 {
			err = io.EOF
		} else {
			f.readdirLast = res[len(res)-1].Name()
		}
	}

	return res, err
}

// Readdirnames reads and returns a slice of names from the directory f.
//
// If n > 0, Readdirnames returns at most n names. In this case, if Readdirnames
// returns an empty slice, it will return a non-nil error explaining why. At the
// end of a directory, the error is io.EOF.
//
// If n <= 0, Readdirnames returns all the names from the directory in a single
// slice. In this case, if Readdirnames succeeds (reads all the way to the end
// of the directory), it returns the slice and a nil error. If it encounters an
// error before the end of the directory, Readdirnames returns the names read
// until that point and a non-nil error.
func (f *FileReader) Readdirnames(n int) ([]string, error) {
	if f.closed {
		return nil, io.ErrClosedPipe
	}

	fis, err := f.Readdir(n)
	if err != nil {
		return nil, err
	}

	names := make([]string, 0, len(fis))
	for _, fi := range fis {
		names = append(names, fi.Name())
	}

	return names, nil
}

// Close implements io.Closer.
func (f *FileReader) Close() error {
	f.closed = true

	if f.blockReader != nil {
		f.blockReader.Close()
	}

	return nil
}

func (f *FileReader) getBlocks() error {
	req := &hdfs.GetBlockLocationsRequestProto{
		Src:    proto.String(f.name),
		Offset: proto.Uint64(0),
		Length: proto.Uint64(uint64(f.info.Size())),
	}
	resp := &hdfs.GetBlockLocationsResponseProto{}

	err := f.client.namenode.Execute("getBlockLocations", req, resp)
	if err != nil {
		return err
	}

	f.blocks = resp.GetLocations().GetBlocks()
	return nil
}

func (f *FileReader) getNewBlockReader() error {
	off := uint64(f.offset)
	for _, block := range f.blocks {
		start := block.GetOffset()
		end := start + block.GetB().GetNumBytes()

		if start <= off && off < end {
			br := rpc.NewBlockReader(block, int64(off-start), f.client.namenode.ClientName())

			f.blockReader = br
			return nil
		}
	}

	return fmt.Errorf("Couldn't find block for offset: %d", off)
}
