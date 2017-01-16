# This make file is not necessary
# Just to demonstrate the right process
all: build

.PHONY : clean build_mr build run_local run_distributed

clean:
	rm -f mr/mr
	rm -f word_count_in_go

build_mr:
	cd mr; go build

build: build_mr
	go build

run_local: build
	./word_count_in_go -distributed false

run_distributed: build
	./word_count_in_go -distributed true
