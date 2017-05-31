HADOOP_COMMON_PROTOS = $(shell find protocol/hadoop_common -name '*.proto')
HADOOP_HDFS_PROTOS = $(shell find protocol/hadoop_hdfs -name '*.proto')
GENERATED_PROTOS = $(shell echo "$(HADOOP_HDFS_PROTOS) $(HADOOP_COMMON_PROTOS)" | sed 's/\.proto/\.pb\.go/g')
SOURCES = $(shell find . -name '*.go') $(GENERATED_PROTOS)

# Protobuf needs one of these for every 'import "foo.proto"' in .protoc files.
PROTO_MAPPING = MSecurity.proto=github.com/colinmarc/hdfs/protocol/hadoop_common

all: hdfs

%.pb.go: $(HADOOP_HDFS_PROTOS) $(HADOOP_COMMON_PROTOS)
	protoc --go_out='$(PROTO_MAPPING):protocol/hadoop_common' -Iprotocol/hadoop_common -Iprotocol/hadoop_hdfs $(HADOOP_COMMON_PROTOS)
	protoc --go_out='$(PROTO_MAPPING):protocol/hadoop_hdfs' -Iprotocol/hadoop_common -Iprotocol/hadoop_hdfs $(HADOOP_HDFS_PROTOS)

clean-protos:
	find . -name *.pb.go | xargs rm

hdfs: get-deps clean $(SOURCES)
	go build ./cmd/hdfs

install: get-deps
	go install ./...

test: hdfs
	go test -v -race ./...
	bats ./cmd/hdfs/test/*.bats

clean:
	rm -f ./hdfs

get-deps:
	go get github.com/golang/protobuf/proto
	go get github.com/pborman/getopt
	go get github.com/stretchr/testify/assert
	go get github.com/stretchr/testify/require

.PHONY: clean clean-protos install test get-deps
