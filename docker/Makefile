BINARY = gleam
SOURCE_DIR = github.com/chrislusf/gleam/distributed/gleam

all: docker

.PHONY : clean docker run_docker

clean:
	docker rmi chrislusf/gleam

docker:
	cd ../ && docker build -t chrislusf/gleam -f docker/Dockerfile  .

push: docker
	docker push chrislusf/gleam:latest

run_docker: docker
	docker-compose up