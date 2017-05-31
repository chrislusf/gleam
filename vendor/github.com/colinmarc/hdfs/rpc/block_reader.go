package rpc

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"net"

	hdfs "github.com/colinmarc/hdfs/protocol/hadoop_hdfs"
	"github.com/golang/protobuf/proto"
)

// BlockReader implements io.ReadCloser, for reading a block. It abstracts over
// reading from multiple datanodes, in order to be robust to connection
// failures, timeouts, and other shenanigans.
type BlockReader struct {
	clientName string
	block      *hdfs.LocatedBlockProto
	datanodes  *datanodeFailover
	stream     *blockReadStream
	conn       net.Conn
	offset     int64
	closed     bool
}

// NewBlockReader returns a new BlockReader, given the block information and
// security token from the namenode. It will connect (lazily) to one of the
// provided datanode locations based on which datanodes have seen failures.
func NewBlockReader(block *hdfs.LocatedBlockProto, offset int64, clientName string) *BlockReader {
	locs := block.GetLocs()
	datanodes := make([]string, len(locs))
	for i, loc := range locs {
		datanodes[i] = getDatanodeAddress(loc)
	}

	return &BlockReader{
		clientName: clientName,
		block:      block,
		datanodes:  newDatanodeFailover(datanodes),
		offset:     offset,
	}
}

// Read implements io.Reader.
//
// In the case that a failure (such as a disconnect) occurs while reading, the
// BlockReader will failover to another datanode and continue reading
// transparently. In the case that all the datanodes fail, the error
// from the most recent attempt will be returned.
//
// Any datanode failures are recorded in a global cache, so subsequent reads,
// even reads for different blocks, will prioritize them lower.
func (br *BlockReader) Read(b []byte) (int, error) {
	if br.closed {
		return 0, io.ErrClosedPipe
	} else if uint64(br.offset) >= br.block.GetB().GetNumBytes() {
		br.Close()
		return 0, io.EOF
	}

	// This is the main retry loop.
	for br.stream != nil || br.datanodes.numRemaining() > 0 {
		// First, we try to connect. If this fails, we can just skip the datanode
		// and continue.
		if br.stream == nil {
			err := br.connectNext()
			if err != nil {
				br.datanodes.recordFailure(err)
				continue
			}
		}

		// Then, try to read. If we fail here after reading some bytes, we return
		// a partial read (n < len(b)).
		n, err := br.stream.Read(b)
		br.offset += int64(n)
		if err != nil && err != io.EOF {
			br.stream = nil
			br.datanodes.recordFailure(err)
			if n > 0 {
				return n, nil
			}

			continue
		}

		return n, err
	}

	err := br.datanodes.lastError()
	if err == nil {
		err = errors.New("No available datanodes for block.")
	}

	return 0, err
}

// Close implements io.Closer.
func (br *BlockReader) Close() error {
	br.closed = true
	if br.conn != nil {
		br.conn.Close()
	}

	return nil
}

// connectNext pops a datanode from the list based on previous failures, and
// connects to it.
func (br *BlockReader) connectNext() error {
	address := br.datanodes.next()

	conn, err := net.DialTimeout("tcp", address, connectTimeout)
	if err != nil {
		return err
	}

	err = br.writeBlockReadRequest(conn)
	if err != nil {
		return err
	}

	resp, err := readBlockOpResponse(conn)
	if err != nil {
		return err
	} else if resp.GetStatus() != hdfs.Status_SUCCESS {
		return fmt.Errorf("Error from datanode: %s (%s)", resp.GetStatus().String(), resp.GetMessage())
	}

	readInfo := resp.GetReadOpChecksumInfo()
	checksumInfo := readInfo.GetChecksum()

	var checksumTab *crc32.Table
	checksumType := checksumInfo.GetType()
	switch checksumType {
	case hdfs.ChecksumTypeProto_CHECKSUM_CRC32:
		checksumTab = crc32.IEEETable
	case hdfs.ChecksumTypeProto_CHECKSUM_CRC32C:
		checksumTab = crc32.MakeTable(crc32.Castagnoli)
	default:
		return fmt.Errorf("Unsupported checksum type: %d", checksumType)
	}

	chunkSize := int(checksumInfo.GetBytesPerChecksum())
	stream := newBlockReadStream(conn, chunkSize, checksumTab)

	// The read will start aligned to a chunk boundary, so we need to seek forward
	// to the requested offset.
	amountToDiscard := br.offset - int64(readInfo.GetChunkOffset())
	if amountToDiscard > 0 {
		_, err := io.CopyN(ioutil.Discard, stream, amountToDiscard)
		if err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}

			conn.Close()
			return err
		}
	}

	br.stream = stream
	br.conn = conn
	return nil
}

// A read request to a datanode:
// +-----------------------------------------------------------+
// |  Data Transfer Protocol Version, int16                    |
// +-----------------------------------------------------------+
// |  Op code, 1 byte (READ_BLOCK = 0x51)                      |
// +-----------------------------------------------------------+
// |  varint length + OpReadBlockProto                         |
// +-----------------------------------------------------------+
func (br *BlockReader) writeBlockReadRequest(w io.Writer) error {
	needed := br.block.GetB().GetNumBytes() - uint64(br.offset)
	op := &hdfs.OpReadBlockProto{
		Header: &hdfs.ClientOperationHeaderProto{
			BaseHeader: &hdfs.BaseHeaderProto{
				Block: br.block.GetB(),
				Token: br.block.GetBlockToken(),
			},
			ClientName: proto.String(br.clientName),
		},
		Offset: proto.Uint64(uint64(br.offset)),
		Len:    proto.Uint64(needed),
	}

	return writeBlockOpRequest(w, readBlockOp, op)
}
