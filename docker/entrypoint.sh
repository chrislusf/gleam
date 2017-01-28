#!/bin/sh

case "$1" in

  'master')
  	exec /usr/bin/gleam $@
	;;

  'agent')
  	ARGS="--host=`hostname -i`  --dir=/data"
	# Is this instance linked with a master? (Docker commandline "--link master1:master")
  	if [ -n "$MASTER_ADDR" ] ; then
		ARGS="$ARGS --master=$MASTER_ADDR:45326"
	fi
  	exec /usr/bin/gleam $@ $ARGS
	;;

  *)
  	exec /usr/bin/gleam $@
	;;
esac
