package util

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

func grpcDial(address string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	// opts = append(opts, grpc.WithBlock())
	// opts = append(opts, grpc.WithTimeout(time.Duration(5*time.Second)))
	var options []grpc.DialOption
	options = append(options,
		// grpc.WithInsecure(),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:    30 * time.Second, // client ping server if no activity for this long
			Timeout: 20 * time.Second,
		}))
	for _, opt := range opts {
		if opt != nil {
			options = append(options, opt)
		}
	}
	return grpc.Dial(address, options...)
}

func GleamGrpcDial(address string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	grpcAddress, err := ParseServerToGrpcAddress(address)
	if err != nil {
		return nil, err
	}
	return grpcDial(grpcAddress, opts...)
}

func ParseServerToGrpcAddress(server string) (serverGrpcAddress string, err error) {
	colonIndex := strings.LastIndex(server, ":")
	if colonIndex < 0 {
		return "", fmt.Errorf("server should have hostname:port format: %v", server)
	}

	port, parseErr := strconv.ParseUint(server[colonIndex+1:], 10, 64)
	if parseErr != nil {
		return "", fmt.Errorf("server port parse error: %v", parseErr)
	}

	grpcPort := int(port) + 10000

	return fmt.Sprintf("%s:%d", server[:colonIndex], grpcPort), nil
}
