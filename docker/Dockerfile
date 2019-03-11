FROM alpine:latest
MAINTAINER Chris Lu <chris.lu@gmail.com>

EXPOSE 45326
EXPOSE 55326

VOLUME /data

COPY gleam /usr/bin/
COPY entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
