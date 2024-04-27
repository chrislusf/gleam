FROM golang:alpine AS builder
WORKDIR /go/src/github.com/chrislusf/gleam
COPY . .
RUN go mod download
RUN go build -o /go/bin/gleam ./distributed/gleam

FROM alpine:latest
MAINTAINER Chris Lu <chris.lu@gmail.com>

EXPOSE 45326
EXPOSE 55326

VOLUME /data

COPY --from=builder /go/bin/gleam /usr/bin/
COPY ./docker/entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]