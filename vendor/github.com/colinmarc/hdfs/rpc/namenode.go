package rpc

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"

	hadoop "github.com/colinmarc/hdfs/protocol/hadoop_common"
	"github.com/golang/protobuf/proto"
)

const (
	rpcVersion           = 0x09
	serviceClass         = 0x0
	authProtocol         = 0x0
	protocolClass        = "org.apache.hadoop.hdfs.protocol.ClientProtocol"
	protocolClassVersion = 1
	handshakeCallID      = -3
)

// NamenodeConnection represents an open connection to a namenode.
type NamenodeConnection struct {
	clientId         []byte
	clientName       string
	currentRequestID int
	user             string
	conn             net.Conn
	reqLock          sync.Mutex
}

// NamenodeError represents an interepreted error from the Namenode, including
// the error code and the java backtrace.
type NamenodeError struct {
	Method    string
	Message   string
	Code      int
	Exception string
}

// Desc returns the long form of the error code, as defined in the
// RpcErrorCodeProto in RpcHeader.proto
func (err *NamenodeError) Desc() string {
	return hadoop.RpcResponseHeaderProto_RpcErrorCodeProto_name[int32(err.Code)]
}

func (err *NamenodeError) Error() string {
	s := fmt.Sprintf("%s call failed with %s", err.Method, err.Desc())
	if err.Exception != "" {
		s += fmt.Sprintf(" (%s)", err.Exception)
	}

	return s
}

// NewNamenodeConnection creates a new connection to a Namenode, and preforms an
// initial handshake.
//
// You probably want to use hdfs.New instead, which provides a higher-level
// interface.
func NewNamenodeConnection(address, user string) (*NamenodeConnection, error) {
	conn, err := net.DialTimeout("tcp", address, connectTimeout)
	if err != nil {
		return nil, err
	}

	return WrapNamenodeConnection(conn, user)
}

// WrapNamenodeConnection wraps an existing net.Conn to a Namenode, and preforms
// an initial handshake.
//
// You probably want to use hdfs.New instead, which provides a higher-level
// interface.
func WrapNamenodeConnection(conn net.Conn, user string) (*NamenodeConnection, error) {
	// The ClientID is reused here both in the RPC headers (which requires a
	// "globally unique" ID) and as the "client name" in various requests.
	clientId := newClientID()
	c := &NamenodeConnection{
		clientId:   clientId,
		clientName: "go-hdfs-" + string(clientId),
		user:       user,
		conn:       conn,
	}

	err := c.writeNamenodeHandshake()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("Error performing handshake: %s", err)
	}

	return c, nil
}

// ClientName provides a unique identifier for this client, which is required
// for various RPC calls. Confusingly, it's separate from clientID, which is
// used in the RPC header; to make things simpler, it reuses the random bytes
// from that, but adds a prefix to make it human-readable.
func (c *NamenodeConnection) ClientName() string {
	return c.clientName
}

// Execute performs an rpc call. It does this by sending req over the wire and
// unmarshaling the result into resp.
func (c *NamenodeConnection) Execute(method string, req proto.Message, resp proto.Message) error {
	c.reqLock.Lock()
	defer c.reqLock.Unlock()

	c.currentRequestID++
	err := c.writeRequest(method, req)
	if err != nil {
		c.conn.Close()
		return err
	}

	err = c.readResponse(method, resp)
	if err != nil {
		if _, ok := err.(*NamenodeError); !ok {
			c.conn.Close() // TODO don't close on RPC failure
		}

		return err
	}

	return nil
}

// RPC definitions

// A request packet:
// +-----------------------------------------------------------+
// |  uint32 length of the next three parts                    |
// +-----------------------------------------------------------+
// |  varint length + RpcRequestHeaderProto                    |
// +-----------------------------------------------------------+
// |  varint length + RequestHeaderProto                       |
// +-----------------------------------------------------------+
// |  varint length + Request                                  |
// +-----------------------------------------------------------+
func (c *NamenodeConnection) writeRequest(method string, req proto.Message) error {
	rrh := newRPCRequestHeader(c.currentRequestID, c.clientId)
	rh := newRequestHeader(method)

	reqBytes, err := makeRPCPacket(rrh, rh, req)
	if err != nil {
		return err
	}

	_, err = c.conn.Write(reqBytes)
	return err
}

// A response from the namenode:
// +-----------------------------------------------------------+
// |  uint32 length of the next two parts                      |
// +-----------------------------------------------------------+
// |  varint length + RpcResponseHeaderProto                   |
// +-----------------------------------------------------------+
// |  varint length + Response                                 |
// +-----------------------------------------------------------+
func (c *NamenodeConnection) readResponse(method string, resp proto.Message) error {
	var packetLength uint32
	err := binary.Read(c.conn, binary.BigEndian, &packetLength)
	if err != nil {
		return err
	}

	packet := make([]byte, packetLength)
	_, err = io.ReadFull(c.conn, packet)
	if err != nil {
		return err
	}

	rrh := &hadoop.RpcResponseHeaderProto{}
	err = readRPCPacket(packet, rrh, resp)

	if rrh.GetStatus() != hadoop.RpcResponseHeaderProto_SUCCESS {
		return &NamenodeError{
			Method:    method,
			Message:   rrh.GetErrorMsg(),
			Code:      int(rrh.GetErrorDetail()),
			Exception: rrh.GetExceptionClassName(),
		}
	} else if int(rrh.GetCallId()) != c.currentRequestID {
		return errors.New("Error reading response: unexpected sequence number")
	}

	return nil
}

// A handshake packet:
// +-----------------------------------------------------------+
// |  Header, 4 bytes ("hrpc")                                 |
// +-----------------------------------------------------------+
// |  Version, 1 byte (default verion 0x09)                    |
// +-----------------------------------------------------------+
// |  RPC service class, 1 byte (0x00)                         |
// +-----------------------------------------------------------+
// |  Auth protocol, 1 byte (Auth method None = 0x00)          |
// +-----------------------------------------------------------+
// |  uint32 length of the next two parts                      |
// +-----------------------------------------------------------+
// |  varint length + RpcRequestHeaderProto                    |
// +-----------------------------------------------------------+
// |  varint length + IpcConnectionContextProto                |
// +-----------------------------------------------------------+
func (c *NamenodeConnection) writeNamenodeHandshake() error {
	c.reqLock.Lock()
	defer c.reqLock.Unlock()

	rpcHeader := []byte{
		0x68, 0x72, 0x70, 0x63, // "hrpc"
		rpcVersion, serviceClass, authProtocol,
	}

	rrh := newRPCRequestHeader(handshakeCallID, c.clientId)
	cc := newConnectionContext(c.user)
	packet, err := makeRPCPacket(rrh, cc)
	if err != nil {
		return err
	}

	_, err = c.conn.Write(append(rpcHeader, packet...))
	return err
}

// Close terminates all underlying socket connections to remote server.
func (c *NamenodeConnection) Close() error {
	return c.conn.Close()
}

func newRPCRequestHeader(id int, clientID []byte) *hadoop.RpcRequestHeaderProto {
	return &hadoop.RpcRequestHeaderProto{
		RpcKind:  hadoop.RpcKindProto_RPC_PROTOCOL_BUFFER.Enum(),
		RpcOp:    hadoop.RpcRequestHeaderProto_RPC_FINAL_PACKET.Enum(),
		CallId:   proto.Int32(int32(id)),
		ClientId: clientID,
	}
}

func newRequestHeader(methodName string) *hadoop.RequestHeaderProto {
	return &hadoop.RequestHeaderProto{
		MethodName:                 proto.String(methodName),
		DeclaringClassProtocolName: proto.String(protocolClass),
		ClientProtocolVersion:      proto.Uint64(uint64(protocolClassVersion)),
	}
}

func newConnectionContext(user string) *hadoop.IpcConnectionContextProto {
	return &hadoop.IpcConnectionContextProto{
		UserInfo: &hadoop.UserInformationProto{
			EffectiveUser: proto.String(user),
		},
		Protocol: proto.String(protocolClass),
	}
}
