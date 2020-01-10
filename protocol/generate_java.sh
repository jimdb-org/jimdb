#!/bin/bash

mkdir ../jimsql/jim-sdk/jim-proto/src/main/proto
cp -rf ../jim-protocol/*pb ../jimsql/jim-sdk/jim-proto/src/main/proto
cd ../jimsql/jim-sdk/jim-proto/src/main

sed -i '/gogo/d' `grep -rl gogo proto`
sed -i 's/github.com\/chubaodb\/chubaodb\/proto\///' `grep -rl github.com proto`
sed -i "/package/a\\option java_package=\"io.chubao.jim.pb\";" proto/*/*.proto
sed -i "/java_package/a\\option java_outer_classname=\"Exprpb\";" proto/dspb/expr.proto
sed -i "/java_package/a\\option java_outer_classname=\"Errorpb\";" proto/dspb/error.proto

cd proto

protoc --java_out=../java ./*/*.proto

rm -rf ../proto
