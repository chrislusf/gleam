BINARY = gleam
SOURCE_DIR = github.com/chrislusf/gleam/distributed/gleam

all: build

.PHONY : clean deps build docker

clean:
	go clean $(SOURCE_DIR)
	rm -f $(BINARY)

deps:
	go get $(GO_FLAGS) -d $(SOURCE_DIR)

build: deps
	GOOS=linux GOARCH=amd64 go build -o $(BINARY) $(SOURCE_DIR)

docker: build
	docker build -t gleam .

