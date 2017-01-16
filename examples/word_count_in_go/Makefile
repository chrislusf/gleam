all: build

.PHONY : clean build run_local run_distributed

clean:
	rm -f go_mapper
	rm -f word_count_local
	rm -f word_count_distributed

build:
	go build -o go_mapper go_mapper.go
	go build -o word_count_local word_count_local.go
	go build -o word_count_distributed word_count_distributed.go

run_local: build
	./word_count_local

run_distributed: build
	./word_count_distributed
