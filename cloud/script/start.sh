#!/bin/bash

echo "how to use: docker run -v ds.conf:/ds.conf ansj/chubaodb:[tagname]"

case $1 in
	ds)
	/jimkv/bin/data-server -c /ds.conf
	;;
	ms)
	/jimkv/bin/master-server  -conf /config.toml
	;;
	proxy)
	echo "not implement"
	;;
	*)
	echo "you must set type to start [ds] [ms] or [proxy]" 
	;;
esac
