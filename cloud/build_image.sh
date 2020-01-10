#!/bin/bash

docker run -it -v $(dirname "$PWD"):/src/github.com/jimdb-org/jimdb ansj/centos7_golang1.12_java8_cmake3.15:0.1 /bin/bash -c 'cd /src/github.com/jimdb-org/jimdb/cloud/script && sh ./build.sh'
