package rpc

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"

	hdfs "github.com/colinmarc/hdfs/protocol/hadoop_hdfs"
	"github.com/golang/protobuf/proto"
)

// ChecksumReader provides an interface for reading the "MD5CRC32" checksums of
// individual blocks. It abstracts over reading from multiple datanodes, in
// order to be robust to failures.
type ChecksumReader struct {
	block     *hdfs.LocatedBlockProto
	datanodes *datanodeFailover

	conn   net.Conn
	reader *bufio.Reader
}

// NewChecksumReader creates a new ChecksumReader for the given block.
func NewChecksumReader(block *hdfs.LocatedBlockProto) *ChecksumReader {
	locs := block.GetLocs()
	datanodes := make([]string, len(locs))
	for i, loc := range locs {
		dn := loc.GetId()
		datanodes[i] = fmt.Sprintf("%s:%d", dn.GetIpAddr(), dn.GetXferPort())
	}

	return &ChecksumReader{
		block:     block,
		datanodes: newDatanodeFailover(datanodes),
	}
}

// ReadChecksum returns the checksum of the block.
func (cr *ChecksumReader) ReadChecksum() ([]byte, error) {
	for cr.datanodes.numRemaining() > 0 {
		address := cr.datanodes.next()
		checksum, err := cr.readChecksum(address)
		if err != nil {
			cr.datanodes.recordFailure(err)
			continue
		}

		return checksum, nil
	}

	err := cr.datanodes.lastError()
	if err != nil {
		err = errors.New("No available datanodes for block.")
	}

	return nil, err
}

func (cr *ChecksumReader) readChecksum(address string) ([]byte, error) {
	conn, err := net.DialTimeout("tcp", address, connectTimeout)
	if err != nil {
		return nil, err
	}

	cr.conn = conn
	err = cr.writeBlockChecksumRequest()
	if err != nil {
		return nil, err
	}

	cr.reader = bufio.NewReader(conn)
	resp, err := cr.readBlockChecksumResponse()
	if err != nil {
		return nil, err
	}

	return resp.GetChecksumResponse().GetMd5(), nil
}

// A checksum request to a datanode:
// +-----------------------------------------------------------+
// |  Data Transfer Protocol Version, int16                    |
// +-----------------------------------------------------------+
// |  Op code, 1 byte (CHECKSUM_BLOCK = 0x55)                  |
// +-----------------------------------------------------------+
// |  varint length + OpReadBlockProto                         |
// +-----------------------------------------------------------+
func (cr *ChecksumReader) writeBlockChecksumRequest() error {
	header := []byte{0x00, dataTransferVersion, checksumBlockOp}

	op := newChecksumBlockOp(cr.block)
	opBytes, err := makePrefixedMessage(op)
	if err != nil {
		return err
	}

	req := append(header, opBytes...)
	_, err = cr.conn.Write(req)
	if err != nil {
		return err
	}

	return nil
}

// The response from the datanode:
// +-----------------------------------------------------------+
// |  varint length + BlockOpResponseProto                     |
// +-----------------------------------------------------------+
func (cr *ChecksumReader) readBlockChecksumResponse() (*hdfs.BlockOpResponseProto, error) {
	respLength, err := binary.ReadUvarint(cr.reader)
	if err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}

		return nil, err
	}

	respBytes := make([]byte, respLength)
	_, err = io.ReadFull(cr.reader, respBytes)
	if err != nil {
		return nil, err
	}

	resp := &hdfs.BlockOpResponseProto{}
	err = proto.Unmarshal(respBytes, resp)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func newChecksumBlockOp(block *hdfs.LocatedBlockProto) *hdfs.OpBlockChecksumProto {
	return &hdfs.OpBlockChecksumProto{
		Header: &hdfs.BaseHeaderProto{
			Block: block.GetB(),
			Token: block.GetBlockToken(),
		},
	}
}
