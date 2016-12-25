all: gen

.PHONY : gen

gen:
	protoc master_agent.proto --go_out=plugins=grpc:.
