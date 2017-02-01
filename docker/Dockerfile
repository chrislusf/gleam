FROM alpine:latest
MAINTAINER Chris Lu <chris.lu@gmail.com>

ENV LUAJIT_VERSION 2.0.4

RUN apk add --update build-base curl libgcc && \
  wget http://luajit.org/download/LuaJIT-$LUAJIT_VERSION.tar.gz && \
  tar xzf LuaJIT-$LUAJIT_VERSION.tar.gz && \
  cd LuaJIT-$LUAJIT_VERSION && \
  make && \
  make install && \
  cd .. && \
  rm -rf luajit-$LUA_VERSION luajit-$LUA_VERSION.tar.gz && \
  apk del build-base curl && \
  rm -rf /var/cache/apk/*

EXPOSE 45326

VOLUME /data

COPY gleam /usr/bin/
COPY MessagePack.lua /usr/local/share/luajit-$LUAJIT_VERSION/
COPY entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
