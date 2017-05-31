package hdfs

import (
	"io"
	"os"

	hdfs "github.com/colinmarc/hdfs/protocol/hadoop_hdfs"
	"github.com/colinmarc/hdfs/rpc"
	"github.com/golang/protobuf/proto"
)

// A FileWriter represents a writer for an open file in HDFS. It implements
// Writer and Closer, and can only be used for writes. For reads, see
// FileReader and Client.Open.
type FileWriter struct {
	client      *Client
	name        string
	replication int
	blockSize   int64

	block       *hdfs.LocatedBlockProto
	blockWriter *rpc.BlockWriter
	closed      bool
}

// Create opens a new file in HDFS with the default replication, block size,
// and permissions (0644), and returns an io.WriteCloser for writing
// to it. Because of the way that HDFS writes are buffered and acknowledged
// asynchronously, it is very important that Close is called after all data has
// been written.
func (c *Client) Create(name string) (*FileWriter, error) {
	_, err := c.getFileInfo(name)
	if err == nil {
		return nil, &os.PathError{"create", name, os.ErrExist}
	} else if !os.IsNotExist(err) {
		return nil, &os.PathError{"create", name, err}
	}

	defaults, err := c.fetchDefaults()
	if err != nil {
		return nil, err
	}

	replication := int(defaults.GetReplication())
	blockSize := int64(defaults.GetBlockSize())
	return c.CreateFile(name, replication, blockSize, 0644)
}

// CreateFile opens a new file in HDFS with the given replication, block size,
// and permissions, and returns an io.WriteCloser for writing to it. Because of
// the way that HDFS writes are buffered and acknowledged asynchronously, it is
// very important that Close is called after all data has been written.
func (c *Client) CreateFile(name string, replication int, blockSize int64, perm os.FileMode) (*FileWriter, error) {
	createReq := &hdfs.CreateRequestProto{
		Src:          proto.String(name),
		Masked:       &hdfs.FsPermissionProto{Perm: proto.Uint32(uint32(perm))},
		ClientName:   proto.String(c.namenode.ClientName()),
		CreateFlag:   proto.Uint32(1),
		CreateParent: proto.Bool(false),
		Replication:  proto.Uint32(uint32(replication)),
		BlockSize:    proto.Uint64(uint64(blockSize)),
	}
	createResp := &hdfs.CreateResponseProto{}

	err := c.namenode.Execute("create", createReq, createResp)
	if err != nil {
		if nnErr, ok := err.(*rpc.NamenodeError); ok {
			err = interpretException(nnErr.Exception, err)
		}

		return nil, &os.PathError{"create", name, err}
	}

	return &FileWriter{
		client:      c,
		name:        name,
		replication: replication,
		blockSize:   blockSize,
	}, nil
}

// Append opens an existing file in HDFS and returns an io.WriteCloser for
// writing to it. Because of the way that HDFS writes are buffered and
// acknowledged asynchronously, it is very important that Close is called after
// all data has been written.
func (c *Client) Append(name string) (*FileWriter, error) {
	info, err := c.getFileInfo(name)
	if err != nil {
		return nil, &os.PathError{"append", name, err}
	}

	appendReq := &hdfs.AppendRequestProto{
		Src:        proto.String(name),
		ClientName: proto.String(c.namenode.ClientName()),
	}
	appendResp := &hdfs.AppendResponseProto{}

	err = c.namenode.Execute("append", appendReq, appendResp)
	if err != nil {
		if nnErr, ok := err.(*rpc.NamenodeError); ok {
			err = interpretException(nnErr.Exception, err)
		}

		return nil, &os.PathError{"append", name, err}
	}

	req := &hdfs.GetBlockLocationsRequestProto{
		Src:    proto.String(name),
		Offset: proto.Uint64(0),
		Length: proto.Uint64(uint64(info.Size())),
	}
	resp := &hdfs.GetBlockLocationsResponseProto{}

	err = c.namenode.Execute("getBlockLocations", req, resp)
	if err != nil {
		return nil, err
	}

	blocks := resp.GetLocations().GetBlocks()
	f := &FileWriter{
		client:      c,
		name:        name,
		replication: int(appendResp.Stat.GetBlockReplication()),
		blockSize:   int64(appendResp.Stat.GetBlocksize()),
	}
	if len(blocks) == 0 {
		return f, nil
	}
	f.block = blocks[len(blocks)-1]
	f.blockWriter = rpc.NewBlockWriter(f.block, c.namenode, f.blockSize)
	return f, nil
}

// CreateEmptyFile creates a empty file at the given name, with the
// permissions 0644.
func (c *Client) CreateEmptyFile(name string) error {
	f, err := c.Create(name)
	if err != nil {
		return err
	}

	return f.Close()
}

// Write implements io.Writer for writing to a file in HDFS. Internally, it
// writes data to an internal buffer first, and then later out to HDFS. Because
// of this, it is important that Close is called after all data has been
// written.
func (f *FileWriter) Write(b []byte) (int, error) {
	if f.closed {
		return 0, io.ErrClosedPipe
	}

	if f.blockWriter == nil {
		err := f.startNewBlock()
		if err != nil {
			return 0, err
		}
	}

	off := 0
	for off < len(b) {
		n, err := f.blockWriter.Write(b[off:])
		off += n
		if err == rpc.ErrEndOfBlock {
			err = f.startNewBlock()
		}

		if err != nil {
			return off, err
		}
	}

	return off, nil
}

// Close closes the file, writing any remaining data out to disk and waiting
// for acknowledgements from the datanodes. It is important that Close is called
// after all data has been written.
func (f *FileWriter) Close() error {
	if f.closed {
		return io.ErrClosedPipe
	}

	var lastBlock *hdfs.ExtendedBlockProto
	if f.blockWriter != nil {
		// Close the blockWriter, flushing any buffered packets.
		err := f.blockWriter.Close()
		if err != nil {
			return err
		}

		lastBlock = f.block.GetB()
	}

	completeReq := &hdfs.CompleteRequestProto{
		Src:        proto.String(f.name),
		ClientName: proto.String(f.client.namenode.ClientName()),
		Last:       lastBlock,
	}
	completeResp := &hdfs.CompleteResponseProto{}

	err := f.client.namenode.Execute("complete", completeReq, completeResp)
	if err != nil {
		return &os.PathError{"create", f.name, err}
	}

	return nil
}

func (f *FileWriter) startNewBlock() error {
	// TODO: we don't need to wait for previous blocks to ack before continuing

	if f.blockWriter != nil {
		err := f.blockWriter.Close()
		if err != nil {
			return err
		}
	}
	var previous *hdfs.ExtendedBlockProto
	if f.block != nil {
		previous = f.block.GetB()
	}

	addBlockReq := &hdfs.AddBlockRequestProto{
		Src:        proto.String(f.name),
		ClientName: proto.String(f.client.namenode.ClientName()),
		Previous:   previous,
	}
	addBlockResp := &hdfs.AddBlockResponseProto{}

	err := f.client.namenode.Execute("addBlock", addBlockReq, addBlockResp)
	if err != nil {
		if nnErr, ok := err.(*rpc.NamenodeError); ok {
			err = interpretException(nnErr.Exception, err)
		}

		return &os.PathError{"create", f.name, err}
	}

	f.block = addBlockResp.GetBlock()
	f.blockWriter = rpc.NewBlockWriter(f.block, f.client.namenode, f.blockSize)
	return nil
}
