#!/bin/bash
cp -rf ../proto ../dataserver/src/proto
mkdir -p ../dataserver/src/proto/gen
sed -i -E '/gogo/d' `grep -rl gogo ../dataserver/src/proto/proto`
sed -i -E 's/github.com\/chubaodb\/chubaodb\/proto\///' `grep -rl github.com ../dataserver/src/proto/proto`
protoc -I../dataserver/src/proto/proto --cpp_out=../dataserver/src/proto/gen ../dataserver/src/proto/proto/*/*.proto

rm -rf ../dataserver/src/proto/proto
