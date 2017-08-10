all: gen

.PHONY : gen

gen:
	protoc gleam.proto --go_out=plugins=grpc:.
