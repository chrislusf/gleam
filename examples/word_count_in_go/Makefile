# This make file is not necessary
# Just to demonstrate the right process
all: build

.PHONY : clean build_mr build run_local run_distributed

clean:
	rm -f word_count_in_go

build:
	go build

run_local: build
	./word_count_in_go

run_distributed: build
	./word_count_in_go -distributed true
