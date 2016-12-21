all: gen

.PHONY : gen

gen:
	protoc resource.proto --go_out=plugins=grpc:master_rpc
