// Package rpc implements some of the lower-level functionality required to
// communicate with the namenode and datanodes.
package rpc

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"time"

	hdfs "github.com/colinmarc/hdfs/protocol/hadoop_hdfs"
	"github.com/golang/protobuf/proto"
)

const (
	dataTransferVersion = 0x1c
	writeBlockOp        = 0x50
	readBlockOp         = 0x51
	checksumBlockOp     = 0x55
)

var (
	connectTimeout  = 1 * time.Second
	namenodeTimeout = 3 * time.Second
	datanodeTimeout = 3 * time.Second
)

// Used for client ID generation, below.
const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func newClientID() []byte {
	id := make([]byte, 16)

	rand.Seed(time.Now().UTC().UnixNano())
	for i := range id {
		id[i] = chars[rand.Intn(len(chars))]
	}

	return id
}

func makeRPCPacket(msgs ...proto.Message) ([]byte, error) {
	packet := make([]byte, 4, 128)

	length := 0
	for _, msg := range msgs {
		b, err := makePrefixedMessage(msg)
		if err != nil {
			return nil, err
		}

		packet = append(packet, b...)
		length += len(b)
	}

	binary.BigEndian.PutUint32(packet, uint32(length))
	return packet, nil
}

// Doesn't include the uint32 length
func readRPCPacket(b []byte, msgs ...proto.Message) error {
	reader := bytes.NewReader(b)
	for _, msg := range msgs {
		msgLength, err := binary.ReadUvarint(reader)
		if err != nil {
			return err
		}

		if msgLength != 0 {
			msgBytes := make([]byte, msgLength)
			_, err = reader.Read(msgBytes)
			if err != nil {
				return err
			}

			err = proto.Unmarshal(msgBytes, msg)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func makePrefixedMessage(msg proto.Message) ([]byte, error) {
	msgBytes, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}

	lengthBytes := make([]byte, 10)
	n := binary.PutUvarint(lengthBytes, uint64(len(msgBytes)))
	return append(lengthBytes[:n], msgBytes...), nil
}

func readPrefixedMessage(r io.Reader, msg proto.Message) error {
	varintBytes := make([]byte, binary.MaxVarintLen32)
	_, err := io.ReadAtLeast(r, varintBytes, 1)
	if err != nil {
		return err
	}

	respLength, varintLength := binary.Uvarint(varintBytes)
	if varintLength < 1 {
		return io.ErrUnexpectedEOF
	}

	// We may have grabbed too many bytes when reading the varint.
	respBytes := make([]byte, respLength)
	extraLength := copy(respBytes, varintBytes[varintLength:])
	_, err = io.ReadFull(r, respBytes[extraLength:])
	if err != nil {
		return err
	}

	return proto.Unmarshal(respBytes, msg)
}

// A op request to a datanode:
// +-----------------------------------------------------------+
// |  Data Transfer Protocol Version, int16                    |
// +-----------------------------------------------------------+
// |  Op code, 1 byte                                          |
// +-----------------------------------------------------------+
// |  varint length + OpReadBlockProto                         |
// +-----------------------------------------------------------+
func writeBlockOpRequest(w io.Writer, op uint8, msg proto.Message) error {
	header := []byte{0x00, dataTransferVersion, op}
	msgBytes, err := makePrefixedMessage(msg)
	if err != nil {
		return err
	}

	req := append(header, msgBytes...)
	_, err = w.Write(req)
	if err != nil {
		return err
	}

	return nil
}

// The initial response from a datanode, in the case of reads and writes:
// +-----------------------------------------------------------+
// |  varint length + BlockOpResponseProto                     |
// +-----------------------------------------------------------+
func readBlockOpResponse(r io.Reader) (*hdfs.BlockOpResponseProto, error) {
	resp := &hdfs.BlockOpResponseProto{}
	err := readPrefixedMessage(r, resp)

	return resp, err
}

func getDatanodeAddress(datanode *hdfs.DatanodeInfoProto) string {
	id := datanode.GetId()
	return fmt.Sprintf("%s:%d", id.GetIpAddr(), id.GetXferPort())
}
