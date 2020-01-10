#!/bin/bash

base_path=$(dirname $(dirname $(dirname "$PWD")))
dataserver_path="$base_path/chubaodb/dataserver"
master_path="$base_path/chubaodb/master"
proxy_path="$base_path/chubaodb/proxy"

bin_path="$base_path/chubaodb/cloud/bin"

mkdir -p $bin_path

build_master() {
	mkdir -p $master_path && cd $master_path/cmd
	go build -o master-server

	./master-server --version
	cp master-server $bin_path
}



build_dataserver() {
	mkdir -p $dataserver_path && cd $dataserver_path
	mkdir -p build && cd build
	cmake ..
	make -j `nproc`
	
	./data-server --version
	mv data-server $bin_path
}

build_proxy() {
	mkdir -p $proxy_path && cd $proxy_path
	mvn clean -U package -Dmaven.test.skip=true
	mv ./jim-sql/jim-server/target/jim-server.zip $base_path
}


main() {
	
	echo "build ds"
    	build_dataserver

	echo "build master"
	build_master

	echo "build proxy"
	#build_proxy

}

main

