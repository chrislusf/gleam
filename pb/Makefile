all: gen

.PHONY : gen

gen:
	protoc master.proto --go_out=plugins=grpc:.
