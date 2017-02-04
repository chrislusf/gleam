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
	./word_count_in_go -distributed=true

run_in_docker:
	GOOS=linux go build
	docker cp ./word_count_in_go docker_master_1:.
	# somehow the equal sign is required in "-onDocker=true"
	docker exec docker_master_1 ./word_count_in_go -onDocker=true
